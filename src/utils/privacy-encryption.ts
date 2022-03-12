import * as crypto from 'crypto'
import dotenv from 'dotenv'

const IV_LENGTH = 16
const IV_VALUE = Buffer.from([
	0x62,
	0xbf,
	0x26,
	0xca,
	0xe1,
	0x72,
	0xce,
	0x4a,
	0x24,
	0x91,
	0x73,
	0xc3,
	0xa2,
	0xcb,
	0x4e,
	0x21,
])

// 암호화
export function encryptInfo(value: string): string {
	if (String(value) !== value || value === '') {
		return undefined
	}

	try {
		const keyValue = dotenv.config()?.parsed?.INFO_SECRET_KEY

		const iv = IV_VALUE //crypto.randomBytes(IV_LENGTH)

		const cipher = crypto.createCipheriv('aes-256-cbc', keyValue, iv)
		const encrypted = cipher.update(value)

		return (
			iv.toString('hex') +
      ':' +
      Buffer.concat([encrypted, cipher.final()]).toString('hex')
		)
	} catch (error) {
		return value
	}
}

// 복호화
export function decryptInfo(value: string): string {
	if (String(value) !== value || value === '') {
		return value
	}

	try {
		const keyValue = dotenv.config()?.parsed?.INFO_SECRET_KEY

		const textParts = value?.split(':')
		const iv = Buffer.from(textParts.shift(), 'hex')
		const encryptedText = Buffer.from(textParts.join(':'), 'hex')
		const decipher = crypto.createDecipheriv('aes-256-cbc', keyValue, iv)
		const decrypted = decipher.update(encryptedText)

		return Buffer.concat([decrypted, decipher.final()]).toString()
	} catch (error) {
		return value
	}
}
