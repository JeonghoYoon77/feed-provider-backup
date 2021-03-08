class Constants {
  private _limit: number = 99999
  private _cafe24PCAddress: string = 'https://fetching.co.kr/product/detail.html?product_no='
  private _cafe24MobileAddress: string = 'https://m.fetching.co.kr/product/detail.html?product_no='

  limit(): number {
  	return this._limit
  }

  cafe24PCAddress(): string {
  	return this._cafe24PCAddress
  }

  cafe24MobileAddress(): string {
  	return this._cafe24MobileAddress
  }
}

export default Constants
