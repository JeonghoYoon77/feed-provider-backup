import { S3Client as S3 } from '@aws-sdk/client-s3'
import { AWS } from '../config'
import {Upload} from '@aws-sdk/lib-storage'

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
    	const params = {
    		ACL: 'public-read',
    		Bucket: 'fetching-feeds',
    		ContentType: contentType,
    		Key: key,
    		Body: buffer || readStream,
    	}

    	const parallelUploadS3 = new Upload({
    		client,
    		params
    	})

    	await parallelUploadS3.done()

    	return `${this.s3Url}/${folderName}/${fileName}`
    }
}
S3Client.s3Url = AWS.S3.URL
