// Copyright 2019 PingCAP, Inc.

use ferromq_protobuf_build::Builder;

fn main() {
    Builder::new().search_dir_for_protos("proto").generate()
}
