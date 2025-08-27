#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operation {
    Update,
    Delete,
}

impl Operation {
    pub fn name(&self) -> String {
        match self {
            Operation::Update => "UPDATE".to_string(),
            Operation::Delete => "DELETE".to_string(),
        }
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Update => write!(f, "UPDATE"),
            Operation::Delete => write!(f, "DELETE"),
        }
    }
}
