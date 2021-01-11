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

export default {
	PORT,
	ENVIRONMENT,
}
