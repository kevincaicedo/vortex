use crate::context::SmokeContext;

pub type CaseFn = fn(&mut SmokeContext) -> anyhow::Result<()>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CommandGroup {
    String,
    Key,
    Transaction,
    Server,
}

impl CommandGroup {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Key => "key",
            Self::Transaction => "transaction",
            Self::Server => "server",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SupportLevel {
    Supported,
    Partial,
    Stubbed,
}

impl SupportLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Partial => "partial",
            Self::Stubbed => "stubbed",
        }
    }
}

#[derive(Clone)]
pub struct CaseDef {
    pub name: &'static str,
    pub summary: &'static str,
    pub run: CaseFn,
}

impl CaseDef {
    pub const fn new(name: &'static str, summary: &'static str, run: CaseFn) -> Self {
        Self { name, summary, run }
    }
}

#[derive(Clone)]
pub struct CommandSpec {
    pub name: &'static str,
    pub group: CommandGroup,
    pub support: SupportLevel,
    pub summary: &'static str,
    pub syntax: &'static [&'static str],
    pub tested: &'static [&'static str],
    pub not_tested: &'static [&'static str],
    pub cases: Vec<CaseDef>,
}

impl CommandSpec {
    pub fn new(name: &'static str, group: CommandGroup, support: SupportLevel) -> Self {
        Self {
            name,
            group,
            support,
            summary: "",
            syntax: &[],
            tested: &[],
            not_tested: &[],
            cases: Vec::new(),
        }
    }

    pub fn summary(mut self, summary: &'static str) -> Self {
        self.summary = summary;
        self
    }

    pub fn syntax(mut self, syntax: &'static [&'static str]) -> Self {
        self.syntax = syntax;
        self
    }

    pub fn tested(mut self, tested: &'static [&'static str]) -> Self {
        self.tested = tested;
        self
    }

    pub fn not_tested(mut self, not_tested: &'static [&'static str]) -> Self {
        self.not_tested = not_tested;
        self
    }

    pub fn case(mut self, case: CaseDef) -> Self {
        self.cases.push(case);
        self
    }
}
