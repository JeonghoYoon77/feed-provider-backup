import { AWS } from '../config'
import S3 from 'aws-sdk/clients/s3'
import * as stream from 'stream'

export class S3Client {
    static client
    static s3Url: string

    static async upload({ folderName, fileName, buffer = null, readStream = null, contentType = 'text/tsv' }) {
    	const key = `${folderName}/${fileName}`

    	const client = new S3({
    		credentials: {
    			accessKeyId: AWS.ACCESS_KEY_ID,
    			secretAccessKey: AWS.SECRET_ACCESS_KEY,
    		}
    	})
    	if (buffer) {
    		const params = {
    			ACL: 'public-read',
    			Bucket: 'fetching-feeds',
    			ContentType: contentType,
    			Key: key,
    			Body: buffer,
    		}
    		await client.putObject(params).promise()
    	} else if (readStream) {
    		const pass = new stream.PassThrough()

    		const params = {
    			ACL: 'public-read',
    			Bucket: 'fetching-feeds',
    			ContentType: contentType,
    			Key: key,
    			Body: pass,
    		}

    		const promise = client.putObject(params)

    		readStream.pipe(pass)

    		await promise
    	}

    	return `${this.s3Url}/${folderName}/${fileName}`
    }
}
S3Client.s3Url = AWS.S3.URL
