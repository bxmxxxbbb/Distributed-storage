/*******************************************
 * des.h
 * 用户使用des算法头文件
 * 
********************************************/

#ifndef _OPENSSL_H_
#define _OPENSSL_H_

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief 加密小于的数据(encrypt: 加密)
 * @param pInData 明文数据
 * @param nInDataLen 明文数据长度
 * @param pOutData 加密后的数据
 * @param pOutDataLen 加密数据的长度
 * @return 是否成功
*/
int DesEnc(unsigned char *pInData, int nInDataLen, unsigned char *pOutData, int *pOutDataLen);

/**
 * @brief 加密等于4k数据
*/
int DesEnc_raw(unsigned char *pInData, int nInDataLen, unsigned char *pOutData, int *pOutDataLen);

/**
 * @brief 解密小于4k的数据(decrypt : 解密)
 * @param pInData 密文数据
 * @param nInDataLen 密文数据长度
 * @param pOutData 解密后的数据
 * @param pOutDataLen 解密数据长度
*/

int DesDec(unsigned char *pInData, int nInDataLen, unsigned char *pOutData, int *pOutDataLen);

/**
 * @brief 解密等于4k的数据
*/
int DesDec_raw(unsigned char *pInData,int nInDataLen, unsigned char *pOutData ,int *pOutDataLen);



#ifdef __cplusplus
}
#endif

#endif