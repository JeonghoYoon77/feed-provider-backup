import { program } from 'commander'
import { version } from '../package.json'
import {
	NaverFeed,
	NaverSalesFeed,
	GoogleFeed,
	KakaoFeed,
	KakaoUpdateFeed,
	FacebookFeed,
	PiclickFeed,
	OrderFeed,
} from './feeds'

program.version(version)
program.option('-f, --feed-name <feedName>', '피드 이름')
program.parse(process.argv)

async function main() {
	const feedName = (program.feedName || '').toUpperCase()
	const feedExecute = {
		'NAVER-FEED': async () => {
			const naverFeed = new NaverFeed()
			await naverFeed.upload()
		},
		'NAVER-SALES-FEED': async () => {
			const naverSalesFeed = new NaverSalesFeed()
			await naverSalesFeed.upload()
		},
		'GOOGLE-FEED': async () => {
			const googleFeed = new GoogleFeed()
			await googleFeed.upload()
		},
		'KAKAO-FEED': async () => {
			const kakaoFeed = new KakaoFeed()
			await kakaoFeed.upload()
		},
		'KAKAO-UPDATE-FEED': async () => {
			const kakaoFeed = new KakaoUpdateFeed()
			await kakaoFeed.upload()
		},
		'FACEBOOK-FEED': async () => {
			await FacebookFeed()
		},
		'PICLICK-FEED': async () => {
			const piclickFeed = new PiclickFeed()
			await piclickFeed.upload()
		},
		'ORDER-FEED': async () => {
			const orderFeed = new OrderFeed()
			await orderFeed.upload()
		},
	}

	try {
		await feedExecute[feedName]()
	} catch (e) {
		console.log(e)
		process.exit(1)
	}

	process.exit(0)
}
main()
