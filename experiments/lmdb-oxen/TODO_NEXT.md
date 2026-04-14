What I want to do next is to go back into the plan and actually implement this.

I understand better how to layout things and feasability of this enum-box tree.
It is bad with memory. We'd have to do some sort of iteration/streaming thing.

What we want is LMDB to be the store from which we stream from.
We need something better in-memory to represent "this is a tree and I can get content from
it from disk." The tree itself can be quite large... maybe it's ok to have it,
but 10M `MerkleTree::Dir` and `MerkleTree::File`s will be **TOO MUCH**.

This is fundamentally THE reason why we have to operate with something where we 
**KEEP GOING BACK TO DISK** to follow the "next" node in the tree.

So what we want is really something like:
```rust

enum Node {
    Dir { name: String, hash: Hash, children: Vec<LazyNode> },
    File { name: String, hash: Hash },
}

struct LazyNode(Arc<LMDB>, ...);

impl Lazynode {
    fn load() -> Node
}
```

I.e. we return a **COROUTINE** that lets us call "next" with LMDB to get the content we are seeking.
