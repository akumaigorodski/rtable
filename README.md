# rtable
Efficient Table data structure similar to Guava Table from Java world.

Can be seen as 2D dictionary where a value lies at the intersection of both row and column keys, is particularly useful for representing certain kinds of graphs.

Additionally it provides O(1) operations for fetching all values associated with a particular row or column value.

```rust
use std::collections::HashSet;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
struct Id(usize);

impl TableKV for Id {
    fn id(&self) -> usize { 
        self.0 
    }
}

fn demo_simple() {
    let mut tbl: Table<Id, Id, Id> = Table::new();

    tbl.insert(Id(1), Id(10), Id(100));
    tbl.insert(Id(1), Id(10), Id(101));
    tbl.insert(Id(2), Id(10), Id(200));
    
    // what values sit at (col=1, row=10)?
    let cell_vals: &HashSet<usize> = tbl.tuples.get(&(1, 10)).unwrap();
    println!("(1,10) → {:?}", cell_vals); // {100, 101}

    // what values appear anywhere in column 1?
    let col1_vals: &HashSet<usize> = tbl.cols2values.get(&1).unwrap();
    println!("col 1 → {:?}", col1_vals);  // {100, 101}

    // remove one value and see it drop out
    tbl.remove(1, 10, 100);
    assert!(tbl.tuples.get(&(1, 10)).unwrap().contains(&101));
    assert!(!tbl.tuples.get(&(1, 10)).unwrap().contains(&100));

    // rebuild the "inverse" diffs for each occupied cell
    let inv = InverseTable::rebuild_from(&tbl);

    // For cell (1, 10): values in col 1 but not in row 10?
    let diff = &inv.column_value_keys_except[&(1,10)];
    println!("col 1 \\ row 10 = {:?}", diff);
}
```