use tikv_jemallocator::Jemalloc;

/// Global jemalloc allocator instance.
///
/// Import this in `vortex-server`'s `main.rs` to activate it:
/// ```ignore
/// use vortex_memory::GlobalAllocator;
/// #[global_allocator]
/// static ALLOC: GlobalAllocator = GlobalAllocator;
/// ```
pub type GlobalAllocator = Jemalloc;
