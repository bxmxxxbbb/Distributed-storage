#ifndef _DEAL_MYSQL_H_
#define _DEAL_MYSQL_H_

#include <mysql/mysql.h>

#define SQL_MAX_LEN (512) //sql语句长度

/**
 * @brief 打印操作数据库出错时的错误信息
 * 
 * @param conn (in)  连接数据库的句柄
 * @param title  (int) 用户错误信息提示
*/

void print_error(MYSQL *conn, const char *title);

/**
 * @brief 连接数据库
 * 
 * @param user_name (in) 数据库用户
 * @param passwd    (in) 数据库密码
 * @param db_name   (in) 数据库名称
 * 
 * @return 
 *          成功：连接数据库的句柄
 *          失败：NULL
*/
MYSQL* mysql_conn(char *user_name, char *passwd, char *db_name);

/**
 * @brief 处理数据库查询结果，结果集保存在buf，只处理一条数据，一个字段，如果buf为NULL，无需保存结果集，只做哦按段有没有此纪录
 * 
 * @return 0 成功并保存记录集，1没有记录集，2有记录集但是没有保存，-1失败
*/
int process_result_one(MYSQL *conn ,char *sql_cmd, char *buf);

#endif