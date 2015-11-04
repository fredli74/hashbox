//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// This is a temporary hack to allow the server exe to create the hashback accounts
// It is a copy of the functions found in hashback client and it should be placed
// in an administrative tool instead

package core

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
)

func GenerateAccessKey(account string, password string) Byte128 {
	return DeepHmac(20000, append([]byte(account), []byte("*ACCESS*KEY*PAD*")...), Hash([]byte(password)))
}
func GenerateBackupKey(account string, password string) Byte128 {
	return DeepHmac(20000, append([]byte(account), []byte("*ENCRYPTION*PAD*")...), Hash([]byte(password)))
}
func GenerateDataEncryptionKey() Byte128 {
	var key Byte128
	rand.Read(key[:])
	return key
}
func DecryptDataInPlace(cipherdata []byte, key Byte128) {
	aesCipher, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}

	aesStream := cipher.NewCBCDecrypter(aesCipher, []byte("*HB*AES*DATA*IV*"))
	aesStream.CryptBlocks(cipherdata, cipherdata)
}
func EncryptDataInPlace(data []byte, key Byte128) {
	aesCipher, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}

	aesStream := cipher.NewCBCEncrypter(aesCipher, []byte("*HB*AES*DATA*IV*"))
	aesStream.CryptBlocks(data, data)
}
