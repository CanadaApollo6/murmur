use std::os::fd::OwnedFd;
use std::sync::Arc;

use crate::surfaces::Result;

pub(crate) fn check_arch() -> Result<()> {
    if cfg!(all(
        target_os = "linux",
        target_endian = "little",
        target_pointer_width = "64",
        any(target_arch = "x86_64", target_arch = "aarch64")
    )) {
        Ok(())
    } else {
        Err(format!(
            "sandbox seccomp requires little-endian Linux x86_64 or aarch64; unsupported architecture {} on {}",
            std::env::consts::ARCH,
            std::env::consts::OS
        )
        .into())
    }
}

#[cfg(not(all(
    target_os = "linux",
    target_endian = "little",
    target_pointer_width = "64",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
pub(crate) fn filter_fd() -> Result<Arc<OwnedFd>> {
    check_arch()?;
    Err("sandbox seccomp unavailable".into())
}

#[cfg(all(
    target_os = "linux",
    target_endian = "little",
    target_pointer_width = "64",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod native {
    use super::*;
    use nix::libc;
    use std::io::{Seek, Write};

    struct DeniedSyscall {
        number: libc::c_long,
        argument: Option<(u32, u32)>,
        errno: i32,
    }

    const DENIED: &[DeniedSyscall] = &[
        DeniedSyscall {
            number: libc::SYS_socket,
            argument: Some((0, libc::AF_VSOCK as u32)),
            errno: libc::EAFNOSUPPORT,
        },
        DeniedSyscall {
            number: libc::SYS_socketpair,
            argument: Some((0, libc::AF_VSOCK as u32)),
            errno: libc::EAFNOSUPPORT,
        },
        DeniedSyscall {
            number: libc::SYS_io_uring_setup,
            argument: None,
            errno: libc::ENOSYS,
        },
        DeniedSyscall {
            number: libc::SYS_io_uring_enter,
            argument: None,
            errno: libc::ENOSYS,
        },
        DeniedSyscall {
            number: libc::SYS_io_uring_register,
            argument: None,
            errno: libc::ENOSYS,
        },
        DeniedSyscall {
            number: libc::SYS_keyctl,
            argument: None,
            errno: libc::ENOSYS,
        },
        DeniedSyscall {
            number: libc::SYS_add_key,
            argument: None,
            errno: libc::ENOSYS,
        },
        DeniedSyscall {
            number: libc::SYS_request_key,
            argument: None,
            errno: libc::ENOSYS,
        },
    ];

    #[cfg(target_arch = "x86_64")]
    const AUDIT_ARCH: u32 = 0xc000_003e;
    #[cfg(target_arch = "aarch64")]
    const AUDIT_ARCH: u32 = 0xc000_00b7;

    fn instruction(code: u32, k: u32, jt: u8, jf: u8) -> libc::sock_filter {
        libc::sock_filter {
            code: code as u16,
            jt,
            jf,
            k,
        }
    }

    // seccompiler 0.5 has one match action per filter and no x32 syscall-bit guard.
    // This deny table needs two errno actions in one program.
    fn program() -> Vec<libc::sock_filter> {
        let load = libc::BPF_LD | libc::BPF_W | libc::BPF_ABS;
        let equal = libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K;
        let ret = libc::BPF_RET | libc::BPF_K;
        let mut bpf = vec![
            instruction(load, 4, 0, 0), // seccomp_data.arch
            instruction(equal, AUDIT_ARCH, 1, 0),
            instruction(ret, libc::SECCOMP_RET_KILL_PROCESS, 0, 0),
            instruction(load, 0, 0, 0), // seccomp_data.nr
        ];
        #[cfg(target_arch = "x86_64")]
        bpf.extend([
            instruction(
                libc::BPF_JMP | libc::BPF_JSET | libc::BPF_K,
                0x4000_0000,
                0,
                1,
            ),
            instruction(ret, libc::SECCOMP_RET_ERRNO | libc::ENOSYS as u32, 0, 0),
        ]);
        for rule in DENIED {
            bpf.push(instruction(
                equal,
                rule.number as u32,
                0,
                if rule.argument.is_some() { 4 } else { 1 },
            ));
            if let Some((index, value)) = rule.argument {
                // Socket domains are int arguments. The kernel ignores their upper 32 bits.
                bpf.push(instruction(load, 16 + index * 8, 0, 0));
                bpf.push(instruction(equal, value, 0, 1));
            }
            bpf.push(instruction(
                ret,
                libc::SECCOMP_RET_ERRNO | rule.errno as u32,
                0,
                0,
            ));
            if rule.argument.is_some() {
                bpf.push(instruction(load, 0, 0, 0));
            }
        }
        bpf.push(instruction(ret, libc::SECCOMP_RET_ALLOW, 0, 0));
        bpf
    }

    pub(crate) fn filter_fd() -> Result<Arc<OwnedFd>> {
        check_arch()?;
        let fd = nix::sys::memfd::memfd_create(
            c"agentd-seccomp",
            nix::sys::memfd::MFdFlags::MFD_CLOEXEC,
        )?;
        let mut file = std::fs::File::from(fd);
        for op in program() {
            file.write_all(&op.code.to_ne_bytes())?;
            file.write_all(&[op.jt, op.jf])?;
            file.write_all(&op.k.to_ne_bytes())?;
        }
        file.rewind()?;
        Ok(Arc::new(file.into()))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        // Evaluate the emitted kernel program on synthetic seccomp_data, including
        // ABIs that the running kernel may not support executing.
        fn action(arch: u32, number: u32, domain: u64) -> u32 {
            let bpf = program();
            let mut data = [0u8; 64];
            data[0..4].copy_from_slice(&number.to_ne_bytes());
            data[4..8].copy_from_slice(&arch.to_ne_bytes());
            data[16..24].copy_from_slice(&domain.to_ne_bytes());
            let mut accumulator = 0;
            let mut pc = 0;
            loop {
                let op = &bpf[pc];
                match u32::from(op.code) {
                    code if code == libc::BPF_LD | libc::BPF_W | libc::BPF_ABS => {
                        let offset = op.k as usize;
                        accumulator = u32::from_ne_bytes(
                            data[offset..offset + 4].try_into().expect("bug: word load"),
                        );
                    }
                    code if code == libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K => {
                        pc += usize::from(if accumulator == op.k { op.jt } else { op.jf });
                    }
                    code if code == libc::BPF_JMP | libc::BPF_JSET | libc::BPF_K => {
                        pc += usize::from(if accumulator & op.k != 0 {
                            op.jt
                        } else {
                            op.jf
                        });
                    }
                    code if code == libc::BPF_RET | libc::BPF_K => return op.k,
                    _ => panic!("bug: unsupported BPF instruction"),
                }
                pc += 1;
            }
        }

        #[test]
        fn denied_abis_cannot_bypass_native_syscall_rules() {
            for arch in [0x4000_0003, 0x4000_0028, AUDIT_ARCH ^ 0x4000_0000] {
                for number in [102, libc::SYS_socket as u32, libc::SYS_getpid as u32] {
                    assert_eq!(action(arch, number, 40), libc::SECCOMP_RET_KILL_PROCESS);
                }
            }
            #[cfg(target_arch = "x86_64")]
            for number in [0, 41, 101, 250, 425, 512, u32::MAX] {
                assert_eq!(
                    action(AUDIT_ARCH, number | 0x4000_0000, 40),
                    libc::SECCOMP_RET_ERRNO | libc::ENOSYS as u32
                );
            }
        }

        #[test]
        fn native_policy_preserves_other_domains_and_syscalls() {
            for number in [libc::SYS_socket, libc::SYS_socketpair] {
                for domain in [1, 2, 10] {
                    assert_eq!(
                        action(AUDIT_ARCH, number as u32, domain),
                        libc::SECCOMP_RET_ALLOW
                    );
                }
                for domain in [40, 0xffff_ffff_0000_0028] {
                    assert_eq!(
                        action(AUDIT_ARCH, number as u32, domain),
                        libc::SECCOMP_RET_ERRNO | libc::EAFNOSUPPORT as u32
                    );
                }
            }
            for number in [libc::SYS_getpid, libc::SYS_connect, libc::SYS_read] {
                assert_eq!(
                    action(AUDIT_ARCH, number as u32, 40),
                    libc::SECCOMP_RET_ALLOW
                );
            }
        }
    }
}

#[cfg(all(
    target_os = "linux",
    target_endian = "little",
    target_pointer_width = "64",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub(crate) use native::filter_fd;
