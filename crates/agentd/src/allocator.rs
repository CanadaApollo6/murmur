// Shared by the daemon and the library test binary containing the worker gate.
#[global_allocator]
pub(crate) static ALLOCATOR: std::alloc::System = std::alloc::System;
