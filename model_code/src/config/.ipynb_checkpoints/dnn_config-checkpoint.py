config={
    "embedding_dim":32,
    "num_embedding":20000,
    "lr":0.001,
    "batch_size":512,
    "num_experts":3,
    "feature_col" :["target_brand_id","clk_brand_seq","clt_brand_seq","pay_brand_seq","clk_cate_seq","clt_cate_seq","pay_cate_seq","user_click_feature","user_clt_feature","user_cart_feature","user_pay_feature","brand_stat_feature","user_cate2_cross_feature","user_brand_cross_feature"],
    "features_gate_col":["target_brand_id","brand_stat_feature","clk_brand_seq","user_cate2_cross_feature","user_brand_cross_feature"]
}