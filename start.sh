#!/bin/bash
# 拷贝nginx的配置文件nginx.conf到默认目录
mv /usr/local/nginx/conf/nginx.conf /usr/local/nginx/conf/nginx.conf.old
cp ./conf/nginx.conf /usr/local/nginx/conf
echo

# 拷贝nginx的fastdfs插件配置文件mod_fastdfs.conf到默认目录
echo =========Copy mod_fastdfs.conf.conf===============
mv /etc/fdfs/mod_fastdfs.conf /etc/fdfs/mod_fastdfs.conf.old
cp ./conf/mod_fastdfs.conf /etc/fdfs
echo

echo =====================fastdfs=======================
# 关闭已启用的 tracker 和 storage
./fastdfs.sh stop
# 启动 tracker 和 storage
./fastdfs.sh all
# 重启所有的cgi程序
echo
echo =====================fastCHI=======================
./fcgi.sh
#关闭nginx
echo
./nginx.sh stop
# 启动nginx
./nginx.sh start
# 关闭redis
echo
echo ====================redis==========================
./redis.sh stop
# 启动redis
./redis.sh start