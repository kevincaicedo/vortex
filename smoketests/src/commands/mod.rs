pub mod append;
pub mod command;
pub mod copy;
pub mod dbsize;
pub mod decr;
pub mod decrby;
pub mod del;
pub mod discard;
pub mod echo;
pub mod exec;
pub mod exists;
pub mod expire;
pub mod expireat;
pub mod expiretime;
pub mod flushall;
pub mod flushdb;
pub mod get;
pub mod getdel;
pub mod getex;
pub mod getrange;
pub mod getset;
pub mod incr;
pub mod incrby;
pub mod incrbyfloat;
pub mod info;
pub mod keys;
pub mod mget;
pub mod mset;
pub mod msetnx;
pub mod multi;
pub mod persist;
pub mod pexpire;
pub mod pexpireat;
pub mod pexpiretime;
pub mod ping;
pub mod psetex;
pub mod pttl;
pub mod quit;
pub mod randomkey;
pub mod rename;
pub mod renamenx;
pub mod scan;
pub mod select;
pub mod set;
pub mod setex;
pub mod setnx;
pub mod setrange;
pub mod strlen;
pub mod time;
pub mod touch;
pub mod ttl;
pub mod r#type;
pub mod unlink;
pub mod unwatch;
pub mod watch;

use crate::spec::CommandSpec;

pub fn all_specs() -> Vec<CommandSpec> {
    let mut specs = vec![
        append::spec(),
        command::spec(),
        copy::spec(),
        dbsize::spec(),
        decr::spec(),
        decrby::spec(),
        del::spec(),
        discard::spec(),
        echo::spec(),
        exec::spec(),
        exists::spec(),
        expire::spec(),
        expireat::spec(),
        expiretime::spec(),
        flushall::spec(),
        flushdb::spec(),
        get::spec(),
        getdel::spec(),
        getex::spec(),
        getrange::spec(),
        getset::spec(),
        incr::spec(),
        incrby::spec(),
        incrbyfloat::spec(),
        info::spec(),
        keys::spec(),
        mget::spec(),
        mset::spec(),
        msetnx::spec(),
        multi::spec(),
        persist::spec(),
        pexpire::spec(),
        pexpireat::spec(),
        pexpiretime::spec(),
        ping::spec(),
        psetex::spec(),
        pttl::spec(),
        quit::spec(),
        randomkey::spec(),
        rename::spec(),
        renamenx::spec(),
        scan::spec(),
        select::spec(),
        set::spec(),
        setex::spec(),
        setnx::spec(),
        setrange::spec(),
        strlen::spec(),
        time::spec(),
        touch::spec(),
        ttl::spec(),
        r#type::spec(),
        unlink::spec(),
        unwatch::spec(),
        watch::spec(),
    ];
    specs.sort_by(|left, right| left.name.cmp(right.name));
    specs
}
