use crate::error::OxenError;

use crate::util;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DFLogicalOp {
    AND,
    OR,
}

impl DFLogicalOp {
    pub fn from_str_op(s: &str) -> DFLogicalOp {
        match s {
            "&&" => DFLogicalOp::AND,
            "||" => DFLogicalOp::OR,
            _ => panic!("Unknown DFLogicalOp"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DFLogicalOp::AND => "&&",
            DFLogicalOp::OR => "||",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DFFilterOp {
    EQ,
    LT,
    GT,
    GTE,
    LTE,
    NEQ,
}

impl DFFilterOp {
    pub fn from_str_op(s: &str) -> DFFilterOp {
        match s {
            "==" => DFFilterOp::EQ,
            "<" => DFFilterOp::LT,
            ">" => DFFilterOp::GT,
            "<=" => DFFilterOp::LTE,
            ">=" => DFFilterOp::GTE,
            "!=" => DFFilterOp::NEQ,
            _ => panic!("Unknown DFFilterOp"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DFFilterOp::EQ => "==",
            DFFilterOp::LT => "<",
            DFFilterOp::GT => ">",
            DFFilterOp::LTE => "<=",
            DFFilterOp::GTE => ">=",
            DFFilterOp::NEQ => "!=",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DFFilterVal {
    pub op: DFFilterOp,
    pub field: String,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct DFFilterExp {
    // logical ops are all the ops we want to chain "&&","||"
    pub logical_ops: Vec<DFLogicalOp>,
    // vals are all the sub expressions like "label != person"
    pub vals: Vec<DFFilterVal>,
}

// Scan query to find the first logical op if there is one
fn find_next_logical_op(
    query: &str,
    logical_ops: &Vec<DFLogicalOp>,
) -> Option<(DFLogicalOp, usize)> {
    let mut logical_op: Option<(DFLogicalOp, usize)> = None;
    let mut min_idx = query.len();
    for op in logical_ops {
        if let Some(idx) = query.find(op.as_str())
            && idx < min_idx
        {
            logical_op = Some((op.to_owned(), idx));
            min_idx = idx;
        }
    }
    logical_op
}

/// Can parse an expression such as "pred_label == person && is_correct == true"
pub fn parse(query: Option<String>) -> Result<Option<DFFilterExp>, OxenError> {
    if let Some(mut filter) = query {
        if filter.is_empty() {
            return Err(OxenError::parse_error(filter));
        }

        // 1) Iterate over string finding DFLogicalOps and collecting the sub expressions
        let mut found_ops: Vec<DFLogicalOp> = vec![];
        let mut sub_exprs: Vec<String> = vec![];
        let candidate_ops = vec![DFLogicalOp::AND, DFLogicalOp::OR];

        while let Some((op, idx)) = find_next_logical_op(&filter, &candidate_ops) {
            // split at idx
            let op_str = op.as_str();
            let (sub_expr, remaining) = filter.split_at(idx);
            let (_, remaining) = remaining.split_at(op_str.len());

            // add sub_expr to eval later
            sub_exprs.push(sub_expr.trim().to_string());

            // add op
            found_ops.push(op);
            filter = remaining.to_string();
        }

        // push remaining
        sub_exprs.push(filter.trim().to_string());

        // 2) Parse each sub expression
        // Order in which we check matters because some ops are substrings of others, put the longest ones first
        let filter_ops = [
            DFFilterOp::NEQ,
            DFFilterOp::GTE,
            DFFilterOp::LTE,
            DFFilterOp::EQ,
            DFFilterOp::GT,
            DFFilterOp::LT,
        ];

        let mut filter_vals: Vec<DFFilterVal> = vec![];
        for sub_expr in sub_exprs {
            for op in filter_ops.iter() {
                if sub_expr.contains(op.as_str()) {
                    let split = util::str::split_and_trim(&sub_expr, op.as_str());
                    let val = DFFilterVal {
                        op: op.clone(),
                        field: split[0].to_owned(),
                        value: split[1].to_owned(),
                    };
                    filter_vals.push(val);
                    break;
                }
            }
        }

        return Ok(Some(DFFilterExp {
            logical_ops: found_ops,
            vals: filter_vals,
        }));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use crate::{
        core::df::filter::{DFFilterOp, DFLogicalOp, parse},
        error::OxenError,
    };

    #[test]
    fn test_parse_filter_single_expr() -> Result<(), OxenError> {
        let query = Some("label == person".to_string());

        let opt = parse(query)?.unwrap();

        assert_eq!(opt.logical_ops.len(), 0);

        assert_eq!(opt.vals.len(), 1);
        assert_eq!(opt.vals[0].field, "label");
        assert_eq!(opt.vals[0].op, DFFilterOp::EQ);
        assert_eq!(opt.vals[0].value, "person");
        Ok(())
    }

    #[test]
    fn test_parse_filter_two_logical_op_expression() -> Result<(), OxenError> {
        let query = Some("label == person && is_true == false".to_string());

        let opt = parse(query)?.unwrap();

        assert_eq!(opt.logical_ops.len(), 1);
        assert_eq!(opt.logical_ops[0], DFLogicalOp::AND);

        assert_eq!(opt.vals.len(), 2);
        assert_eq!(opt.vals[0].field, "label");
        assert_eq!(opt.vals[0].op, DFFilterOp::EQ);
        assert_eq!(opt.vals[0].value, "person");
        assert_eq!(opt.vals[1].field, "is_true");
        assert_eq!(opt.vals[1].op, DFFilterOp::EQ);
        assert_eq!(opt.vals[1].value, "false");
        Ok(())
    }

    #[test]
    fn test_parse_filter_three_logical_op_expression() -> Result<(), OxenError> {
        let query = Some("label == person && min_x > 0 || max_x >= 1.0".to_string());

        let opt = parse(query)?.unwrap();

        assert_eq!(opt.logical_ops.len(), 2);
        assert_eq!(opt.logical_ops[0], DFLogicalOp::AND);
        assert_eq!(opt.logical_ops[1], DFLogicalOp::OR);

        assert_eq!(opt.vals.len(), 3);
        assert_eq!(opt.vals[0].field, "label");
        assert_eq!(opt.vals[0].op, DFFilterOp::EQ);
        assert_eq!(opt.vals[0].value, "person");
        assert_eq!(opt.vals[1].field, "min_x");
        assert_eq!(opt.vals[1].op, DFFilterOp::GT);
        assert_eq!(opt.vals[1].value, "0");
        assert_eq!(opt.vals[2].field, "max_x");
        assert_eq!(opt.vals[2].op, DFFilterOp::GTE);
        assert_eq!(opt.vals[2].value, "1.0");
        Ok(())
    }
}
