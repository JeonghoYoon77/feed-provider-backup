class Constants {
  private _limit: number = 99999
  private _cafe24AddressPrefix: string = 'https://fetching.co.kr/product/detail.html?product_no='

  limit(): number {
  	return this._limit
  }

  cafe24AddressPrefix(): string {
  	return this._cafe24AddressPrefix
  }
}

export default Constants
