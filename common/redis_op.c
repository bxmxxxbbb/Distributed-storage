/**
 * @file redis_op.c
 * @brief  redis操作基本接口和key的操作实现
*/
#include "redis_op.h"
/*-----------------------------------------------------------------------*/
/**
 * @brief 选择redis一个数据库
 * 
 * @param conn 已链接的数据库联俄籍
 * @param db_no redis数据库编号
 * 
 * @return          
 *              -1 失败
 *              0  成功
*/
/*-----------------------------------------------------------------------*/
int rop_selectdatabase(redisContext *conn, unsigned int db_no)
{
    int retn = 0;
    redisReply * reply = NULL;
    /*选择一个数据库*/
    reply = redisCommand(conn, "select %d, db_no");
    if(reply == NULL)
    {
        fprintf(stderr, "[-][GMS_REDIS]Select database %d error!\n",db_no);
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Select database %d error!\n", db_no, conn->errstr);
        retn -1;
        goto END;
    }
    printf("[+][GMS_REDIS]Select database %d SUCCESS!\n", db_no);
    LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]Select database %d SUCCESS!\n",db_no);
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief               清空当前数据库所有信息(慎用)
 * 
 * @param conn          已链接的数据库链接
 * 
 * @return          
 *                  -1      失败
 *                  0       成功
*/
/*-----------------------------------------------------------------------*/
int rop_flush_database(redisContext *conn)
{
    int retn= 0;
    redisReply *reply = NULL;

    reply = redisCommand(conn, "FLUSHDB");
    if(reply == NULL)
    {
        fprintf(stderr, "[-][GMS_REDIS]Clear all data error\n");
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Clear all data error\n");
        retn = -1;
        goto END;
    }
    printf("[+][GMS_REDIS]Clear all data!!\n");
    LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "+][GMS_REDIS]Clear all data!!\n");
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 判断key值是否存在
 * 
 * @param conn 已建立的链接
 * @param key 需要寻找的key值
 * 
 * @return 
 *              -1      失败
 *              1       存在
 *              0       不存在
*/
/*-----------------------------------------------------------------------*/
int rop_is_key_exist(redisContext *conn, char *key)
{
    int retn = 0;
    redisReply *reply = NULL;

    reply = redisCommand(conn, "EXISTS %s", key);
    // rop_test_reply_type(reply);
    if(reply->type != REDIS_REPLY_INTEGER)
    {
        fprintf(stderr, "[-][GMS_REDIS]is key exist get wrong type!\n");
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]is key exist get wrong type! %s\n", conn->errstr);
        retn = -1;
        goto END;
    }

    if(reply->integer == 1)
    {
        retn = 1;
    }
    else 
    {
        retn = 0;
    }
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief           删除一个key
 * 
 * @param conn       已经建立的连接
 * @param key       
 * 
 * @return          
 *                  -1 失败
 *                  0  成功
*/
/*-----------------------------------------------------------------------*/
int rop_del_key(redisContext *conn, char *key)
{
    int retn = 0;
    redisReply * reply = NULL;

    reply = redisCommand(conn, "DEL %s", key);
    if(reply->type != REDIS_REPLY_INTEGER)
    {
        fprintf(stderr, "[-][GMS_REDIS] DEL key %s ERROR\n", key);
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS] DEL key %s ERROR %s\n", key, conn->errstr);
        retn = -1;
        goto END;
    }
    if(reply->integer > 0)
    {
        retn = 0;
    }
    else 
    {
        retn = -1;
    }
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 设置一个key的删除时间，系统到达一定时间
 *                      将会自动删除该KEY
 * @param conn          已经建立好的连接
 * 
 * @param delete_time   到期时间    time_t 日历时间
 * 
 * @return  
 *          0   SUCC
 *          -1  FAIL
*/
/*-----------------------------------------------------------------------*/
int rop_set_key_lifecycle(redisContext *conn, char *key, time_t delete_time)
{
    int retn = 0;
    redisReply *reply = NULL;

    reply = redisCommand(conn, "EXPIREAT %s %d", key, delete_time);
    if(reply->type != REDIS_REPLY_INTEGER)
    {
        fprintf(stderr, "[-][GMS_REDIS]Set key:%s delete time ERROR!\n", key);
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Set key:%s delete time ERROR! %s\n",key, conn->errstr);
        retn = -1;
    }
    if(reply->integer == 1)
    {
        /*成功*/
        retn = 0;
    }
    else 
    {
        /*错误*/
        retn = -1;
    }
    freeReplyObject(reply);

    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 打印数据库中所有匹配pattern的key
 * @param   conn        已建立好的连接
 * @param   pattern     匹配模式，pattern支持glob-style的通配符格式
 *                      如 *表示任意一个或多个字符
 *                      ？表示任意字符
 *                      [abc]表示方括号中的任意一个字符
*/
/*-----------------------------------------------------------------------*/
void rop_show_keys(redisContext *conn, char *pattern)
{
    int i = 0;
    redisReply *reply = NULL;

    reply = redisCommand(conn, "key %s", pattern);
    if(reply->type != REDIS_REPLY_ARRAY)
    {
        fprintf(stderr, " [-][GMS_REDIS]show all key and data wrong type!\n");
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]show all keys and data wrong type! %s\n", conn->errstr);
        goto END;
    }

    for(i = 0; i < reply->elements; ++i)
    {
        printf("======[%s]======\n",reply->element[i]->str);
    }
END:
    freeReplyObject(reply);
}

/*-----------------------------------------------------------------------*/
/**
 * @brief                   批量执行已经封装好的redis命令
 * @param conn              已经建立好的连接
 * @param cmds              封装好的命令行数组
 * @param cmd_num           命令个数
 * 
 * @return              
 *                      -1      fail
 *                      0       succ 
*/
/*-----------------------------------------------------------------------*/
int rop_redis_append(redisContext *conn, RCOMMANDS cmds,int cmd_num)
{
    int retn = 0;
    int i = 0;
    redisReply *reply =NULL;

    /*批量插入命令到缓冲命令管道*/
    for(i = 0 ; i < cmd_num; i++)
    {
        retn = redisAppendCommand(conn, cmds[i]);
        if(retn != REDIS_OK)
        {
            fprintf(stderr, "[-][GMS_REDIS]Append Command: %s ERROR!\n",cmds[i]);
            LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Append Command: %s ERROR! %s \n",cmds[i], conn->errstr);
            retn = -1;
            goto END;
        }
        retn = 0;
    }

    /*提交命令*/
    for(i = 0; i < cmd_num; i++)
    {
        retn = redisGetReply(conn, (void**)&reply);
        if(retn != REDIS_OK)
        {
            retn = -1;
            fprintf(stderr, "[-][GMS_REDIS]Commit Command:%s ERROR\n",cmds[i]);
            LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Commit Command:%s ERROR! %s\n",cmds[i], conn->errstr);
            freeReplyObject(reply);
            break;
        }
        freeReplyObject(reply);
        retn = 0;
    }
END:
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 执行单项命令，无返回值 命令自行输入
 * 
 * @param conn              已经建立好的连接
 * @param cmd               封装好的命令
 * 
 * @return
 *          0  SUCC
 *          -1 FAIL
*/
/*-----------------------------------------------------------------------*/
int rop_redis_command(redisContext *conn, char *cmd)
{
    int retn = 0;

    redisReply *reply = NULL;
    reply = redisCommand(conn, cmd);
    if(reply == NULL)
    {
        LOG(REDIS_LOG_MODULE,REDIS_LOG_PROC, "[-][GMS_REDIS]Command : %s ERROR! %s\n", cmd, conn->errstr);
        retn = -1;
    }
    freeReplyObject(reply);

    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief               测试一个reply的结果类型
 *                      得到对应的烈性用对应的方法获取数据
 * 
 * @param reply         返回的命令结果
*/
/*-----------------------------------------------------------------------*/
void rop_test_reply_type(redisReply *reply)
{
    switch (reply->type)
    {
    case REDIS_REPLY_STATUS:
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]=REDIS_REPLY_STATUS=[string] use reply->str to get data, reply->len get data len\n");
        break;
    case REDIS_REPLY_ERROR:
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]=REDIS_REPLY_ERROR=[string] use reply->str to get data, reply->len get data len\n");
        break;
    case REDIS_REPLY_INTEGER:
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]=REDIS_REPLY_INTEGER=[long long] use reply->integer to get data\n");
        break;
    case REDIS_REPLY_NIL:
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]=REDIS_REPLY_NIL=[] data npt exist\n");
        break;
    case REDIS_REPLY_ARRAY:
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]=REDIS_REPLY_ARRAY=[array] use reply->elements to get number of data, reply->element[index] to get (struct redisReply*) Object\n");
        break;
    case REDIS_REPLY_STRING:
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]=REDIS_REPLY_STRING[string] use reply->str to get data, reply->len data len\n");
        break;
    default:
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Can't pasre this type\n");
        break;
    }
}

/*-----------------------------------------------------------------------*/
/**
 * @brief redis tcp模式连接
 * 
 * @param ip_str    redis服务器IP
 * @param port_str  redis服务器端口
 * 
 * @return 
 *                  成功返回链接句柄
 *                  失败返回NULL
*/
/*-----------------------------------------------------------------------*/
redisContext * rop_connectdb_nopwd(char *ip_str, char * port_str)
{
    redisContext *conn = NULL;
    uint16_t port = atoi(port_str);
    conn = redisConnect(ip_str, port_str);

    if(conn == NULL)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Connect %s:%d Error:Can't allocate redis context!\n", ip_str, port);
        goto END;
    }
    if(conn->err)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Connect %s:%d Error:%s\n", ip_str, port, conn->errstr);
        redisFree(conn);
        conn =  NULL;
        goto END;
    }
    LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]Connect %s:%d SUCCESS!\n", ip_str, port);
END:
    return conn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief               redis tcp模式连接
 * 
 * @param  ip_str       redis服务器ip
 * @param  port_str     redis服务器port
 * @param  pwd          密码
 * 
 * @return
 *                  成功返回连接句柄
 *                  失败返回NULL
*/
/*-----------------------------------------------------------------------*/
redisContext * rop_connectdb(char *ip_str, char *port_str, char *pwd)
{
    redisContext *conn = NULL;
    uint16_t port = atoi(port_str);
    char auth_cmd[REDIS_COMMAND_SIZE];

    conn = redisConnect(ip_str, port);
    if(conn == NULL)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Connect %s:%d Error:Can't allocate redis context!\n",ip_str, port);
        goto END;
    }

    if(conn->err)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Connect %s:%d Error:%s\n",ip_str, port, conn->errstr);
        redisFree(conn);
        conn = NULL;
        goto END;
    }

    redisReply * reply = NULL;
    sprintf(auth_cmd, "AUTH %S", pwd);

    reply = redisCommand(conn, auth_cmd);
    if(reply == NULL)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Command : auth %s ERROR!\n", pwd);
        conn = NULL;
        goto END;
    }
    freeReplyObject(reply);

    LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]Connect %s:%d SUCCESS!\n",ip_str, port);
END:
    return conn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief redis unix域模式连接
 * 
 * @param ip_str  unix域sock文件
 * 
 * @returns 
 *                  成功返回连接句柄
 *                  失败返回NULL
*/
/*-----------------------------------------------------------------------*/
redisContext *rop_connectdb_unix(char *sock_path, char *pwd)
{
    redisContext *conn = NULL;
    char auth_cmd[REDIS_COMMAND_SIZE];

    conn = redisConnectUnix(sock_path);
    if(conn == NULL)
    {
        LOG(REDIS_LOG_MODULE,REDIS_LOG_PROC, "[-][GMS_REDIS]Connect doman-unix:%s Error:Can't allocate redis context!\n",sock_path);
        goto END;
    }

    if(conn->err)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Connect dome-uinx:%s\n", sock_path, conn->errstr);
        redisFree(conn);
        conn = NULL;
        goto END;
    }
    redisReply * reply = NULL;
    sprintf(auth_cmd, " auth %s", pwd);
    reply = redisCommand(conn, auth_cmd);
    if(reply == NULL)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Command : auth %s ERROR!\n", pwd);
        conn = NULL;
        goto END;
    }
    freeReplyObject(reply);

    LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Connect domain-unix:%s SUCCESS!\n",sock_path);
END:
    return conn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief tcp  链接redis超时等待模式， timeval超时链接
 *              返回
 * @param ip_str                redis服务器IP
 * @param port_str              redis服务器端口
 * @param timeval               最大超时等待时间
 * 
 * @returns 
 *                              成功返回链接句柄
 *                              失败返回NULL
*/
/*-----------------------------------------------------------------------*/
redisContext *rop_connectdb_timeout(char * ip_str, char *port_str, struct  timeval *timeout)
{
    redisContext *conn = NULL;
    uint16_t port = atoi(port_str);

    conn = redisConnectWithTimeout(ip_str, port, *timeout);

    if(conn == NULL)
    {
        LOG( REDIS_LOG_MODULE, REDIS_LOG_PROC,"[-][GMS_REDIS]Connect %s:%d Error:Can't allocate redis context!\n", ip_str, port);
        goto END;
    }

    if(conn->err)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Connect %s:%d Error:%s\n", ip_str, port, conn->errstr);
        redisFree(conn);
        goto END;
    }

    LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]Connect %s:%d SUCCESS !\n", ip_str, port);
END:
    return conn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 关闭指定的链接句柄
 * 
 * @param conn 已建立好的链接
*/
/*-----------------------------------------------------------------------*/
void rop_disconnect(redisContext *conn)
{
    if( conn == NULL)
    {
         return;
    }
    redisFree(conn);

    LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[+][GMS_REDIS]Disconnect SUCCESS!\n");
}

/*-----------------------------------------------------------------------*/
/*封装一个hmset命令*/
/*-----------------------------------------------------------------------*/
static char * make_hmset_command(char* key, unsigned int element_num, RFIELDS fields, RVALUES values)
{
    char *cmd = NULL;
    unsigned int buf_size = 0;
    unsigned int use_size = 0;
    unsigned int i = 0;

    cmd = (char *)malloc(1024 * 1024);
    if(cmd == NULL)
    {
        goto END;
    }
    memset(cmd, 0, 1024 * 1024);
    buf_size += 1024 * 1024;

    strncat(cmd, "hmset", 6);
    use_size += 5;
    strncat(cmd, " ", 1);
    use_size += 1;

    strncat(cmd, key, 200);
    use_size += 200;
    for(i = 0; i < element_num; ++i)
    {
        strncat(cmd, " ", 1);
        use_size += 1;
        if(use_size >= buf_size)
        {
            cmd = realloc(cmd, use_size + 1024 * 1024);
            if(cmd == NULL)
            {
                goto END;
            }
            buf_size += 1024 * 1024;
        }

        strncat(cmd, fields[i], FIELD_ID_SIZE);
        use_size += strlen(fields[i]);
        if(use_size >= buf_size)
        {
            cmd = realloc(cmd, use_size + 1024 * 1024);
            if(cmd == NULL)
            {
                goto END;
            }
            buf_size += 1024 * 1024;
        }

        strncat(cmd, " ", 1);
        use_size += 1;
        if(use_size >= buf_size)
        {
            cmd = realloc(cmd, use_size + 1024 * 1024);
            if(cmd == NULL)
            {
                goto END;
            }
            buf_size += 1024 * 1024;
        }

        strncat(cmd, values[i], VALUES_ID_SIZE);
        use_size += strlen(values[i]);
        if(use_size >= buf_size)
        {
            cmd = realloc(cmd, use_size + 1024 * 1024);
            if(cmd == NULL)
            {
                goto END;
            }
            buf_size += 1024 * 1024;
        }
    }
END:
    return cmd;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief               批量执行链表插入命令 插入人链表头部
 * 
 * @param   conn        已建立好的链接
 * @param   key         链表名
 * @param   values      封装好的值数组
 * @param   val_num     值个数
 * 
 * @returns 
 *              0       success
 *              -1      fail
*/
/*-----------------------------------------------------------------------*/
int rop_hash_set_append(redisContext *conn, char *key, RFIELDS fields, RVALUES values, int val_num)
{
    int retn = 0;
    int i = 0;
    redisReply *reply = NULL;

    /*批量插入命令到缓冲命令管道*/
    for(i = 0; i < val_num; i++)
    {
        retn = redisAppendCommand(conn, "hset %s %s %s", key, fields[i], values[i]);
        if(retn != REDIS_OK)
        {
            LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]HSET %s %s %s ERROR![%s]\n", key, fields[i], values[i], conn->errstr);
            retn = -1;
            goto END;
        }
        retn = 0;
    }
    /*提交命令*/
    for(i = 0; i < val_num; i++)
    {
        retn = redisGetReply(conn, (void**)&reply);
        if(retn != REDIS_OK)
        {
            retn = -1;
            LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Commit HSET %s %s %s ERROR!\n", key, fields[i], values[i], conn->errstr);
            freeReplyObject(reply);
            break;
        }
        freeReplyObject(reply);
        retn = 0;
    }
END:
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 向一个hash表中添加一条 key-value 数据
 * 
 * @param conn          redis连接
 * @param key           哈希表名
 * @param field
 * @param value
 * 
 * @returns 
 *                      0   SUCC
 *                      -1  FAIL
*/
/*-----------------------------------------------------------------------*/
int rop_hash_set(redisContext *conn, char *key, char *field, char *value)
{
    int retn = 0;
    redisReply * reply = NULL;

    reply = redisCommand(conn, "hset %s %s %s",key, field, value);
    if(reply == NULL || reply->type != REDIS_REPLY_INTEGER)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]hset %s %s %s error %s\n", key, field, value, conn->errstr);
        retn = -1;
        goto END;
    }
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 从一个hash表中取出一条key-value数据
 * 
 * @param conn redis连接
 * @param key 哈希表名
 * @param field 字段名称
 * @param value 得到的数据， 需要先开辟内存
 * 
 * @returns 
 *          0           SUCCESS
 *          -1          FAIL
*/
/*-----------------------------------------------------------------------*/
int rop_hash_get(redisContext *conn, char *key, char *field, char *value)
{
    int retn = 0;
    int len = 0;

    redisReply *reply= NULL;

    reply = redisCommand(conn, "hget %s %s", key, field);
    if(reply == NULL || reply->type != REDIS_REPLY_STRING)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]hget %s %s error%s\n", key, field, conn->errstr);
        retn = -1;
        goto END;
    }

    len = reply->len > VALUES_ID_SIZE? VALUES_ID_SIZE:reply->len;

    strncpy(value, reply->str, len);
    value[len] = '\0';
END:
    freeReplyObject(reply);

    return retn;
}
/*-----------------------------------------------------------------------*/
/**
 * @brief 从key指定的哈希集中一处指定的域(字段)
 * 
 * @param conn      redis连接
 * @param key       哈希表名
 * @param field     字段
 * 
 * @returns 
 *                  0       SUCCESS
 *                  -1      FAIL
*/
/*-----------------------------------------------------------------------*/
int rop_hash_del(redisContext *conn, char *key, char *field)
{
    int retn = 0;
    redisReply * reply = NULL;

    reply = redisCommand(conn, "hdel %s %s", key, field);
    if(reply->integer != -1)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]hdel %s %s %s error %s\n", key, field, conn->errstr);
        retn = -1;
        goto END;
    }
END:
    freeReplyObject(reply);

    return retn;
}
/*-----------------------------------------------------------------------*/
/**
 * @brief                   创建或者覆盖一个HASH表
 * 
 * @param conn              已建立好的链接
 * @param key               hash表名
 * @param element_num       hash表区域个数
 * @param field             hash表区域名称数组char(*)[FIELD_ID_SIZE]
 * @param values            hash表区域值数组  char(*)[VALUES_ID_SIZE]
 * 
 * @returns
 *                          0       成功
 *                          -1      失败
*/
/*-----------------------------------------------------------------------*/
int rop_create_or_replace_hash_table(redisContext *conn, char *key, unsigned int element_num, RFIELDS fields, RVALUES values)
{
    int retn = 0;
    redisReply *reply = NULL;

    char *cmd = make_hmset_command(key, element_num, fields, values);
    if(cmd == NULL)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]create hash table %s error\n", key);
        retn = -1;
        goto END_WITHOUT_FREE;
    }

    reply = redisCommand(conn, cmd);
    // rop_test_reply_type(reply);
    if(strcmp(reply->str, "OK") != 0)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC,"[-][GMS_REDIS]Create hash table %s Error:%s,%s\n", key, reply->str, conn->errstr);
        return -1;
        goto END;
    }
END:
    free(cmd);
    freeReplyObject(reply);
END_WITHOUT_FREE:
    return retn;
}
/*-----------------------------------------------------------------------*/
/**
 * @brief           给指定的hash表 指定的field对应的value自增num
 * 
 * @param  conn     已建立好的链接
 * @param  key      hash表名
 * @param  field    hash表下的区域名
 * 
 * @returns
 *          0           success
 *          -1          fail
*/
/*-----------------------------------------------------------------------*/
int rop_hincrement_one_field(redisContext *conn, char *key, char *field, unsigned int num)
{
    int retn = 0;
    redisReply *reply = NULL;

    reply = redisCommand(conn, "HINCRBY %s %s %d", key, field, num);
    if(reply == NULL)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]increment %s %s error %s\n", key, field, conn->errstr);
        retn = -1;
        goto END;
    }
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 批量执行链表插入命令 插入链表头部
 * 
 * @param conn          已经建立好得链接
 * @param key           链表名
 * @param values        封装好得值数组
 * @param val_num       值的个数
 * 
 * @returns
 *                      0       succ
 *                      -1      FAIL
*/
/*-----------------------------------------------------------------------*/
int rop_list_push_append(redisContext *conn, char *key, RVALUES values, int val_num)
{
    int retn = 0;
    int i = 0;
    redisReply * reply = NULL;

    /*批量插入命令到缓冲命令管道*/
    for(i = 0; i < val_num; i++)
    {
        retn = redisAppendCommand(conn, "lpush %s %s", key, values[i]);
        if(retn != REDIS_OK)
        {
            LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]PLUSH %s %s ERROR! %s\n",key, values[i], conn->errstr);
            retn = -1;
            goto END;
        }
        retn = 0;
    }
    /*提交命令*/
    for(i = 0; i < val_num; i++)
    {
        retn = redisGetReply(conn, (void**)&reply);
        if(retn != REDIS_OK)
        {
            retn = -1;
            LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Commit LPUSH %s %s ERROR! %s\n",key, conn->errstr);
            freeReplyObject(reply);
            break;
        }
        freeReplyObject(reply);
        retn = 0;
    }
END:
    return retn;
}
/*-----------------------------------------------------------------------*/
/**
 * @brief 单条数据插入链表
 * 
 * @param conn      已经建立好的链接
 * @param key       链表名
 * @param value     数据
 * 
 * @returns 
 *          
*/
/*-----------------------------------------------------------------------*/
int rop_list_push(redisContext *conn, char *key, char *value)
{
    int retn = 0;
    redisReply * reply = NULL;
    reply = redisCommand(conn, "LPUSH %s %s", key, value);
    // rop_test_reply_type(reply);
    if(reply->type != REDIS_REPLY_INTEGER)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]LPUSH %s %s error! %s \n", key, value, conn->errstr);
        retn = -1;
    }
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 得到链表中的元素个数
 * 
 * @param conn 链接句柄
 * @param key   链表名
 * 
 * @returns         
 *              >= 0 个数
 *              -1   失败
*/
/*-----------------------------------------------------------------------*/
int rop_set_list_cnt(redisContext *conn, char *key)
{
    int cnt = 0;
    redisReply *reply = NULL;
    reply = redisCommand(conn, "LLEN %s", key);
    if(reply->type != REDIS_REPLY_INTEGER)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]LLEN %s %s error %s\n", key, conn->errstr);
        cnt = -1;
        goto END;
    }
    cnt = reply->integer;
END:
    freeReplyObject(reply);
    return cnt;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 按照一点个范围截断链表中的数据
 * 
 * @param conn      已建立好的链接
 * @param key       链表名
 * @param begin     阶段起始位置从0开始
 * @param end       阶段结束位置从-1开始
 * 
 *                  这里的范围定义举例
 *                  如果得到的全部范围(0,-1)
 *                  除了最后一个元素范围(0,-2)
 *                  前20个数据范围(0,19)
 * @returns
 *                  0 SUCCESS
 *                  -1 FAIL
*/
/*-----------------------------------------------------------------------*/
int rop_trim_list(redisContext * conn, char *key, int begin, int end)
{
    int retn = 0;
    redisReply *reply = NULL;
    reply = redisCommand(conn, "LTRIM %s %d %d", key, begin, end);
    if(reply->type != REDIS_REPLY_STATUS)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]LTRIM %s %d %d error! %s\n", key, begin, end, conn->errstr);
        retn = -1;
    }
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief rop_range_list 得到链表中的数据
 *                  返回数据为      区间为
 *                  [from_pos      , end_pos]
 * @param conn      已经建立好的链接
 * @param key       表名
 * @param from_pos  查找表的起始数据下标
 * @param end_pos   查找表的结尾数据下标
 * @param values    得到表中的value数据
 * @param get_num   得到结果value的个数
 * 
 * @return      
 *                  0       SUCCESS
 *                  -1      FAIL
*/
/*-----------------------------------------------------------------------*/
int rop_range_list(redisContext *conn, char *key, int from_pos, int end_pos, RVALUES values, int *get_num)
{
    int retn = 0;
    redisReply *reply = NULL;
    int i = 0;
    int max_count = 0;
    int count = end_pos - from_pos + 1;

    reply = redisCommand(conn, "LRANGE %s %d %d", key, from_pos, end_pos);
    if(reply->type !=  REDIS_REPLY_ARRAY || reply->elements == 0)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]LRANGE %S error! %s\n", key, conn->errstr);
        retn = -1;
        goto END;
    }
    max_count = (reply->elements > count) ? count : reply->elements;
    *get_num = max_count;
    
    for(i = 0; i < max_count; i++)
    {
        strncpy(values[i], reply->element[i]->str, VALUES_ID_SIZE - 1);
    }
END:
    if(reply != NULL)
    {
        freeReplyObject(reply);
    }
    return retn;
}

int rop_set_string(redisContext *conn, char *key, char *value)
{
    int retn = 0;
    redisReply *reply = NULL;
    reply = redisCommand(conn, "set %s %s", key, value);
    // rop_test_reply_type(reply);
    if(strcmp(reply->str, "OK") != 0)
    {
        retn = -1;
        goto END;
    }
    // printf("%s\n",reply->str);
END:
    freeReplyObject(reply);
    return retn;
}

int rop_setex_string(redisContext *conn, char *key, unsigned int second, char * value)
{
    int retn = 0;
    redisReply *reply = NULL;
    reply = redisCommand(conn, "setex %s %u %s", key, second, value);
    // rop_test_reply_type(reply);
    if(strcmp(reply->str, "OK") != 0)
    {
        retn = -1;
        goto END;
    }
    // printf("%s\n", reply->str);
END:
    freeReplyObject(reply);
    return retn;
}

int rop_get_string(redisContext *conn, char *key, char *value)
{
    int retn = 0;
    redisReply *reply = NULL;
    reply = redisCommand(conn, "get %s", key);
    // rop_test_reply_type(reply);
    if(reply->type != REDIS_REPLY_STRING)
    {
        retn = -1;
        goto END;
    }
    strncpy(value, reply->str, reply->len);
    value[reply->len] = '\0'; //字符串结束符
END:
    freeReplyObject(reply);
    return retn;
}

// ===========================有序结合相关操作==========================
/*-----------------------------------------------------------------------*/
/**
 * @brief           将指定得zset表，添加新成员
 *                  (key 或者成员不存在则创建)
 * @param   conn    已建立得链接
 * @param   key     zset表名
 * @param   score   zset分数(权重)
 * @param   member  zset成员名
 * 
 * @returns     
 *          0   succ
 *          -1  fail
*/
/*-----------------------------------------------------------------------*/
int rop_zset_add(redisContext *conn, char *key, long score, char * member)
{
    int retn = 0;
    redisReply *reply = NULL;

    // 执行命令，reply->integer成功返回1，reply->integer失败返回零
    reply = redisCommand(conn, "ZADD %s %ld %s", key, score, member);
    // rop_test_reply_type(reply);

    if(reply->integer != 1)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]ZADD: %s, member: %s Error: %s, %s\n",key, member, reply->str, conn->errstr);
        retn = -1;
        goto END;
    }
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 删除指定的成员
 * @param   conn        已建立的链接
 * @param   key         zset表名
 * @param   member      zset成员名
 * 
 * @returns 
 *              0               succ
 *              -1              fail
*/
/*-----------------------------------------------------------------------*/
int rop_zset_zrem(redisContext *conn, char* key, char *member)
{
    int retn = 0;
    redisReply *reply = NULL;
    // 执行命令，reply->integer成功返回1， reply->integer失败返回0
    reply = redisCommand(conn, "ZREM %s %s", key, member);
    // rop_test_reply_type(reply);
    if(reply->integer != 1)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]ZREM: %s, member: %s Error: %s, %s\n",key, member, reply->str, conn->errstr);
        retn = -1;
        goto END;
    }
END:    
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief   删除所有成员
 * 
 * @param   conn    已经建立的连接
 * @param   key     zset表名
 * 
 * @returns 
 *          0           succ
 *          -1          fail
*/
/*-----------------------------------------------------------------------*/
int rop_zset_del_all(redisContext * conn, char *key)
{
    int retn = 0;
    redisReply *reply = NULL;

    // 执行命令
    reply = redisCommand(conn, "ZREMRANGEBYRANK %s 0 -1", key);
    // rop_test_reply_type(reply);
    if(reply->type != REDIS_REPLY_INTEGER)  //如果不是整形
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GSM_REDIS]ZREMRANGEBYRANK: %s, Error: %s, %s\n", key, reply->str, conn->errstr);
        retn = -1;
        goto END;
    }
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief   降序获取有序集合的元素
 *              返回数据为  区间为
 *              [from_pos, end_pos]
 * @param   conn        已建立好的连接
 * @param   key         表名
 * @param   from_pos    查找表的起始数据下标
 * @param   end_pos     查找表的结尾数据下标
 * @param   values      得到表中的value数据
 *              RVALUE为数组指针类型，其变量指向 char[1024]
 *              typedef char (*RVALUES)[VALUES_ID_SIZE]
 * @param   get_num     得到结果value的个数
 * @returns 
 *          0   succ        -1 fail
*/
/*-----------------------------------------------------------------------*/
int rop_zset_zrevrange(redisContext *conn, char *key, int from_pos, int end_pos, RVALUES values, int *get_num)
{
    int retn = 0;
    int i= 0;
    redisReply *reply = NULL;
    int max_count = 0;
    int count = end_pos - from_pos + 1; //请求下载元素个数

    // 降序获取有序集合元素
    reply = redisCommand(conn, "ZREVRANGE %s %d %d", key, from_pos, end_pos);
    if(reply->type != REDIS_REPLY_ARRAY) //如果返回不是数组
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]ZREVRANGE %s error!%s\n", key, conn->errstr);
        retn = -1;
        goto END;
    }
    // 返回一个数组,查看elements的值(数组个数)
    // 通过element[index]的方式访问数组元素
    // 每个数组元素是一个redisReply对象的指针

    max_count = (reply->elements > count) ? count : reply->elements;
    *get_num = max_count; //得到结果value的个数
    for(i = 0; i < max_count; i++)
    {
        strncpy(values[i], reply->element[i]->str, VALUES_ID_SIZE - 1);
        values[i][VALUES_ID_SIZE - 1] = '\0' //结束符
    }
END:    
    if(reply != NULL)
    {
        freeReplyObject(reply);
    }
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief           将指定的zset表,对应的成员,值自增1
 *                  (key 或 成员不存在 则自建)
 * @param   conn    已经建立好的链接
 * @param   key     zset表名
 * @param   member  zset成员名
 * 
 * @returns 
 *              0       succ        -1      fail
*/
/*-----------------------------------------------------------------------*/
int rop_zset_increment(redisContext *conn, char *key, char *member)
{
    int retn = 0;
    redisReply *reply = NULL;
    reply = redisCommand(conn, "ZINCRBY %s 1 %s", key, member);
    // rop_zset_reply_type(reply);
    if(strcmp(reply->str, "OK") != 0)
    {
        log(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Add or increment table: %s, member : %s Error: %s, %s\n",key, member, reply->str, conn->errstr);
        retn = -1;
        goto END;
    }
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief       得到集合中元素的个数
 * @param   conn        已链接好的句柄
 * @param   key         链表名
 * @returns 
 *          >= 0    个数
 *          -1      fail
*/
/*-----------------------------------------------------------------------*/
int rop_zset_zcard(redisContext *conn, char *key)
{
    int cnt = 0;
    redisReply *reply = NULL;

    reply = redisCommand(conn, "ZCARD %s", key);
    if(reply->type != REDIS_REPLY_INTEGER)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]ZCARD %s error %s\n", key, conn->errstr);
        cnt = -1;
        goto END;
    }
END:
    freeReplyObject(reply);
    return cnt;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 得到zset一个member的score
 * 
 * @param   conn    已建立的链接
 * @param   key     zset表名
 * @param   member  zset成员名
 * 
 * @returns 
 *              >= 0        succ
 *              -1          fail
*/
/*-----------------------------------------------------------------------*/
int rop_zset_get_score(redisContext *conn, char *key, char *member)
{
    int score = 0;
    redisReply * reply = NULL;
    reply = redisCommand(conn, "ZSCORE  %s %s", key, member);
    rop_test_reply_type(reply);

    if(reply->type != REDIS_REPLY_STRING){
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]ZSCORE %s %s error %s\n", key, member, conn->errstr);
        score = -1;
        goto END;
    }
    score = atoi(reply->str);
END:
    freeReplyObject(reply);
    return score;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 判断某个成员是否存在
 * 
 * @param   conn    已经建立好的链接
 * @param   key     zset表名
 * @param   member  zset成员名
 * @returns     
 *          0       不存在
 *          1       存在
 *          -1      出错
*/
/*-----------------------------------------------------------------------*/
int rop_zset_exit(redisContext *conn, char *key, char *member)
{
    int retn = 0;
    redisReply *reply = NULL;

    // 执行命令
    reply = reidsCommand(conn, "zlexcount %s [%s [%s",key, member, member);
    // rop_test_reply_type(reply);
    if(reply->type != REDIS_REPLY_INTEGER)
    {
        LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]zlexcount: %s,member: %s Error:%s,%s\n", key, member,reply->str, conn->errstr);
        retn = -1;
        goto END;
    }
    retn = reply->integer;
END:
    freeReplyObject(reply);
    return retn;
}

/*-----------------------------------------------------------------------*/
/**
 * @brief 批量将指定的zset表,对应的成员,值自增1
 *          (key 或 成员不存在则创建)
 * @param   conn        已建立好的链接
 * @param   key         有序集合名称
 * @param   values      封装好的成员数组
 * @param   val_num     数据个数
 * 
 * @returns 
 *              0       succ
 *              -1      fail
*/
/*-----------------------------------------------------------------------*/
int rop_zset_increment_append(redisContext *conn, char *key, RVALUES values, int val_num)
{
    int retn = 0;
    int i = 0;
    redisReply *reply = NULL;

    // 批量命令到缓冲管道
    for (i = 0 ; i < val_num; i++)
    {
        retn = redisAppendCommand(conn, "ZINCRBY %s 1 %s", key, values[i]);
        if(retn != REDIS_OK)
        {
            LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]ZINCRBY %s 1 %s ERROR! %s\n", key, values[i], conn->errstr);
            retn = -1;
            goto END;
        }
        retn = 0;
    }

    // 提交命令
    for(i = 0 ; i < val_num; i++)
    {
        retn = redisGetReply(conn, (void**)&reply);
        if(retn != REDIS_OK)
        {
            retn = -1;
            LOG(REDIS_LOG_MODULE, REDIS_LOG_PROC, "[-][GMS_REDIS]Commit ZINCRBY %s 1 %s ERROR!%s\n", key, values[i], conn->errstr);
            freeReplyObject(reply);
            break;
        }
        freeReplyObject(reply);
        retn = 0;
    }
END:
    return retn;
}