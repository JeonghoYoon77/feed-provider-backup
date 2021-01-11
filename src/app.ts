import express from 'express'
import feeds from './feeds'
import { PORT, ENVIRONMENT } from './config'
import { HTTP_STATUS_CODE, ENVIRONMENT as ENV } from './utils/constants'
import packageInfo from '../package.json'

class App {
    public application: express.Application;
    constructor() {
    	this.application = express()
    }
}

const app = new App().application
async function bootstrap(app: express.Application) {
	app.get('/', (req: express.Request, res: express.Response) => {
		res.json({ version: packageInfo.version })
	})
	// Feeds
	app.use('/feeds', feeds)

	// Error Handlers
	app.use((req: express.Request, res: express.Response) => {
		res.status(HTTP_STATUS_CODE.NotFound).send('404 Not Found')
	})

	app.use((err: Error, req: express.Request, res: express.Response, next: express.NextFunction) => {
		res.status(HTTP_STATUS_CODE.InternalServerError)
			.json({
				message: '예상치 못한 오류가 발생했습니다. 확인 후 빠르게 해결하겠습니다! 🙇‍',
			})
  
		next(err)
	})
}
bootstrap(app)

if (ENVIRONMENT !== ENV.TEST) {
	app.listen(PORT, () => console.log(`Feed Provider listening on port ${PORT}`))
}

export default app
