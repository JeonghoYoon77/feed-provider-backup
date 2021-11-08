function Query(limit: number): string {
	return `
SELECT
  ii.idx,
  ii.item_name,
  ii.item_description,
  IF(cud.product_no, ip.final_price, iup.total_price) as final_price,
  cud.product_no,
  ii.image_url,
  ii.brand_name
FROM item_info ii

JOIN naver_upload_list nul
ON nul.item_id = ii.idx

LEFT JOIN cafe24_upload_db cud
ON cud.item_id = ii.idx AND cud.is_active = 1

JOIN item_show_price isp
ON ii.idx = isp.item_id

JOIN item_user_price isp
ON ii.idx = isp.item_id

JOIN item_price ip
ON ii.idx = ip.item_id AND ip.price_rule = isp.price_rule 

WHERE ii.is_sellable = 1
AND ii.item_priority > 0
ORDER BY ii.item_priority DESC
LIMIT ${limit};
`
}

export default Query
