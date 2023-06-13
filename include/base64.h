#ifndef  BASE64_H
#define  BASE64_H

/**
 * @brief bindata编码成base64
 * @param bindata 源字符串
 * @param binlength 源字符串长度
 * @param base64 目的字符串，base64字符串
 * 
 * @return char* base64字符串
*/
char *base64_encode(const unsigned char *bindata,int binlength, char *base64);

/**
 * @brief 解码base64
 * @param base64 源字符串
 * @param bindata 目的字符串
 * @return int 目的字符串长度
*/
int base64_decode(const char *base64, unsigned char * bindata);
#endif