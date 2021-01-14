export interface iFeed {
	upload()
	getTsvBuffer(): Buffer | Promise<Buffer>
}
