nohup java `cat conf/druid/coordinator/jvm.config | xargs` -cp conf/druid/_common:conf/druid/coordinator:lib/* io.druid.cli.Main server coordinator > var/coordinator.out 2>&1 &
