import { program } from 'commander'
import { version } from '../package.json'
import {
	NaverFeed,
	GoogleFeed,
} from './feeds'

program.version(version)
program
	.option('-f, --feed-name <feedName>', '크롤러 이름')
program.parse(process.argv)

async function main() {
	const feedName = (program.feedName || '').toUpperCase()

	try {
		if (feedName === 'NAVER-FEED') {
			const naverFeed = new NaverFeed()
			await naverFeed.upload()
		}

		if (feedName === 'GOOGLE-FEED') {
			const googleFeed = new GoogleFeed()
			await googleFeed.upload()
		}
	} catch (e) {
		console.log(e)
		process.exit(1)
	}

	process.exit(0)
}
main()
