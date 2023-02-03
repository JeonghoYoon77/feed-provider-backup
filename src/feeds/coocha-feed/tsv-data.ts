type TSVDataCategoryItem = {
  category1: string
  category2: string
  category3: string
  category4: string
}

type TSVDataCategory = {
  category: TSVDataCategoryItem
}

type TSVDataShopItem = {
  'shop_name': string
  'shop_tel': string
  'shop_address': string
  'shop_latitude': string
  'shop_longitude': string
}

type TSVDataShop = {
  shop: TSVDataShopItem
}

type TSVData = {
  'product_id': string
  'product_title': string
  'product_desc': string
  'product_url': string
  'mobile_url': string
  'sale_start': string
  'sale_end': string
  'price_normal': string
  'price_discount': string
  'discount_rate': string
  'coupon_use_start': string
  'coupon_use_end': string
  categorys: TSVDataCategory
  'buy_limit': string
  'buy_max': string
  'buy_count': string,
  'free_shipping': string,
  'image_url1': string
  shops: TSVDataShop
  'm_dcratio': string
  'm_dcprice': string
}

export default TSVData
