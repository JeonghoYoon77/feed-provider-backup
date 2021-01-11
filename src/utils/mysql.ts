import { createPool } from 'mysql2/promise'
import bluebird from 'bluebird'
import { MYSQL } from '../config'

const connectionPool = createPool({
	host: MYSQL.HOST,
	port: MYSQL.PORT,
	database: MYSQL.DATABASE.DEFAULT,
	user: MYSQL.USER,
	password: MYSQL.PASSWORD,
	multipleStatements: true,
	waitForConnections: true,
	Promise: bluebird,
	timezone: '+00:00', // DB에 저장된 시간 그대로 받아오기 위해서
})
export class MySQL {
	/**
	 * @param {string} query
	 * @param {Array} params
	 */
	static async execute(query, params = []) {
		let connection
		try {
			connection = await connectionPool.getConnection()
			const [data] = await connection.query(query, params)
			return data
		} catch (e) {
			console.log(e)
			throw e
		} finally {
			connection.release()
		}
	}
}
