import * as fs from 'fs'

export interface iFeed {
	upload()
	getTsvBuffer(): Buffer | Promise<Buffer> | fs.ReadStream | Promise<fs.ReadStream>
}
