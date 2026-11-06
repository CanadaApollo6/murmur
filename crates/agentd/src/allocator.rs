// Shared by the daemon and the library test binary containing the worker gate.
#[global_allocator]
pub(crate) static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout};
    use std::ffi::c_void;

    unsafe extern "C" {
        fn mi_is_in_heap_region(pointer: *const c_void) -> bool;
    }

    #[test]
    fn rust_uses_mimalloc_while_sqlite_keeps_system_malloc() {
        let layout = Layout::new::<u64>();
        // Initialize the explicit heap before asking its ownership API about pointers.
        let control = unsafe { mimalloc::MiMalloc.alloc(layout) };
        assert!(!control.is_null(), "explicit mimalloc allocation");
        let control_owned = unsafe { mi_is_in_heap_region(control.cast()) };
        unsafe { mimalloc::MiMalloc.dealloc(control, layout) };
        assert!(control_owned, "mimalloc ownership control");

        let rust = Box::new(42_u64);
        let rust_owned = unsafe { mi_is_in_heap_region((&*rust as *const u64).cast()) };
        let sqlite = unsafe { rusqlite::ffi::sqlite3_malloc64(64) };
        assert!(!sqlite.is_null(), "SQLite allocation");
        let sqlite_owned = unsafe { mi_is_in_heap_region(sqlite) };
        unsafe { rusqlite::ffi::sqlite3_free(sqlite) };
        assert!(
            rust_owned,
            "test binary is not using mimalloc for Rust allocations"
        );
        assert!(!sqlite_owned, "SQLite malloc was intercepted by mimalloc");
    }
}
