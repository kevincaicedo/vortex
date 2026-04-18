use anyhow::{Context, Result, bail};
use redis::{Client, Connection, FromRedisValue};

pub struct SmokeContext {
    server_url: String,
    connection: Connection,
}

impl SmokeContext {
    pub fn connect(server_url: &str) -> Result<Self> {
        let client = Client::open(server_url).with_context(|| {
            format!("failed to open redis client for smoke target {server_url}")
        })?;
        let connection = client
            .get_connection()
            .with_context(|| format!("failed to connect to smoke target {server_url}"))?;

        Ok(Self {
            server_url: server_url.to_string(),
            connection,
        })
    }

    pub fn server_url(&self) -> &str {
        &self.server_url
    }

    pub fn reset(&mut self) -> Result<()> {
        let _: () = self.exec(&["FLUSHALL"])?;
        let _: () = self.exec(&["SELECT", "0"])?;
        Ok(())
    }

    pub fn exec<T>(&mut self, args: &[&str]) -> Result<T>
    where
        T: FromRedisValue,
    {
        let cmd = self.build_cmd(args)?;
        cmd.query(&mut self.connection)
            .with_context(|| format!("command failed: {}", args.join(" ")))
    }

    pub fn exec_error(&mut self, args: &[&str]) -> Result<redis::RedisError> {
        let cmd = self.build_cmd(args)?;
        match cmd.query::<redis::Value>(&mut self.connection) {
            Ok(value) => bail!(
                "expected server error for `{}`, got value: {value:?}`",
                args.join(" ")
            ),
            Err(err) => Ok(err),
        }
    }

    pub fn assert_ok(&mut self, args: &[&str]) -> Result<()> {
        let value: String = self.exec(args)?;
        assert_eq!(value, "OK");
        Ok(())
    }

    pub fn assert_nil(&mut self, args: &[&str]) -> Result<()> {
        let value: Option<String> = self.exec(args)?;
        assert_eq!(value, None);
        Ok(())
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.assert_ok(&["SET", key, value])
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        self.exec(&["GET", key])
    }

    pub fn ttl(&mut self, key: &str) -> Result<i64> {
        self.exec(&["TTL", key])
    }

    pub fn pttl(&mut self, key: &str) -> Result<i64> {
        self.exec(&["PTTL", key])
    }

    pub fn dbsize(&mut self) -> Result<i64> {
        self.exec(&["DBSIZE"])
    }

    fn build_cmd(&self, args: &[&str]) -> Result<redis::Cmd> {
        if args.is_empty() {
            bail!("smoke command cannot be empty");
        }

        let mut cmd = redis::cmd(args[0]);
        for arg in &args[1..] {
            cmd.arg(*arg);
        }
        Ok(cmd)
    }
}
