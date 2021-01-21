import { AWS } from '../config'
import S3 from 'aws-sdk/clients/s3'

export class S3Client {
    static client
    static s3Url: string

    static async upload({ folderName, fileName, buffer, contentType = 'text/tsv' }) {
    	const key = `${folderName}/${fileName}`
    	const params = {
    		ACL: 'public-read',
    		Bucket: 'fetching-feeds',
    		ContentType: contentType,
    		Key: key,
    		Body: buffer,
    	}

    	const client = new S3({
    		accessKeyId: AWS.ACCESS_KEY_ID,
    		secretAccessKey: AWS.SECRET_ACCESS_KEY,
    	})
    	await new Promise((resolve, reject) => {
    		client.putObject(params, (error, data) => {
    			if (error) reject(error)
    			resolve(data)
    		})
    	})

    	return `${this.s3Url}/${folderName}/${fileName}`
    }
}
S3Client.s3Url = AWS.S3.URL
