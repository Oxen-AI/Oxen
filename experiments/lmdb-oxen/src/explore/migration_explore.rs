// ==== MIGRATION SCRIPT ====
// walk an oxen repo directory: .oxen/tree/nodes/
//  for each {hash_prefix}
//    for each {hash_suffix}
//      hash = {prefix} + {suffix}
//      look at ./{ha/sh}/nodes and ./{ha/sh}/children
//      create a new entry in nodes table: {hash} -> serialized contents of node file
//      create a new entry in children table: {hash} -> serialized contents of children file
//      create a new entry in dir_hashes table: {hash} -> []each child hash
//
//
// fn main() {
//
// }
