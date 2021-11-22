import {createPool, PoolConnection} from 'mysql2/promise'
import bluebird from 'bluebird'
import { MYSQL } from '../config'

const connectionPool = createPool({
	host: MYSQL.READ_HOST,
	port: MYSQL.PORT,
	database: MYSQL.DATABASE.DEFAULT,
	user: MYSQL.USER,
	password: MYSQL.PASSWORD,
	multipleStatements: true,
	waitForConnections: true,
	Promise: bluebird,
	timezone: '+00:00', // DB에 저장된 시간 그대로 받아오기 위해서
})
const connectionPoolWrite = createPool({
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
process.on('exit', async (code) => {
	await MySQL.dispose()
	await MySQLWrite.dispose()
	console.log('connection closed')
})
export class MySQL {
	/**
	 * @param {string} query
	 * @param {Array} params
	 */
	static async execute(query, params = []): Promise<any[]> {
		let connection: PoolConnection
		try {
			connection = await connectionPool.getConnection()
			const [data]: any[] = await connection.query(connection.format(query, params))
			return data
		} catch (e) {
			console.log(e)
			throw e
		} finally {
			connection.release()
		}
	}

	static async dispose() {
		await connectionPool.end()
	}
}

export class MySQLWrite {
	/**
	 * @param {string} query
	 * @param {Array} params
	 */
	static async execute(query, params = []) {
		let connection
		try {
			connection = await connectionPoolWrite.getConnection()
			const [data] = await connection.query(query, params)
			return data
		} catch (e) {
			console.log(e)
			throw e
		} finally {
			connection.release()
		}
	}

	static async dispose() {
		await connectionPoolWrite.end()
	}
}
