import { config } from 'dotenv'
import { ENVIRONMENT as ENV } from '../utils/constants'

config()

if (process.env.NODE_ENV && process.env.NODE_ENV.toUpperCase() === ENV.TEST) {
	process.env.ENVIRONMENT = ENV.TEST
} else {
	process.env.ENVIRONMENT = (process.env.ENVIRONMENT || ENV.DEVELOPMENT).toUpperCase()
}
export const ENVIRONMENT = process.env.ENVIRONMENT
export const PORT = process.env.PORT || 3000
export const MYSQL = {
	HOST: process.env.MYSQL_HOST,
	PORT: parseInt(process.env.MYSQL_PORT || '3306'),
	DATABASE: {
		DEFAULT: process.env.MYSQL_DATABASE_DEFAULT,
		PARTNERS: process.env.MYSQL_DATABASE_PARTNERS,
	},
	USER: process.env.MYSQL_USER,
	PASSWORD: process.env.MYSQL_PASSWORD,
}

export default {
	PORT,
	ENVIRONMENT,
	MYSQL,
}
