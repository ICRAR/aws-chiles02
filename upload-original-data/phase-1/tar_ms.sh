#!/usr/bin/env bash

tar_ms() {
    date
    cd $1
    tar -cvf - $2 | pigz --best --blocksize 65536 --processes 8 > /mnt/scratch/kvinsen/$2.tar.gz
    date
}

tar_ms /mnt/hidata/chiles/final_products/20131025_951_4_FINAL_PRODUCTS 13B-266.sb27248272.eb28094627.56590.41308579861_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131031_951_4_FINAL_PRODUCTS 13B-266.sb27248272.eb28426877.56596.40353030093_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131109_951_5_FINAL_PRODUCTS 13B-266.sb27243380.eb28499779.56605.3831562037_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131112_951_5_FINAL_PRODUCTS 13B-266.sb27243380.eb28501582.56608.36360891204_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131116_946_6_FINAL_PRODUCTS 13B-266.sb27258103.eb28527118.56612.36334712963_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131117_941_6_FINAL_PRODUCTS 13B-266.sb27261805.eb28527427.56613.350237743056_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131118_946_6_FINAL_PRODUCTS 13B-266.sb27258103.eb28527619.56614.34754607639_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131119_941_6_FINAL_PRODUCTS 13B-266.sb27261805.eb28541939.56615.34480074074_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131121_946_6_FINAL_PRODUCTS 13B-266.sb27258103.eb28547464.56617.33934710648_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131122_941_6_FINAL_PRODUCTS 13B-266.sb27261805.eb28549602.56618.334173599535_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131123_951_6_FINAL_PRODUCTS 13B-266.sb25386827.eb28551343.56619.33367407408_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131203_941_6_FINAL_PRODUCTS 13B-266.sb27261805.eb28559690.56629.30657424768_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131206_951_3_FINAL_PRODUCTS 13B-266.sb25386827.eb28563416.56632.30870270833_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131212_946_4_FINAL_PRODUCTS 13B-266.sb27260261.eb28577580.56638.28133952546_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131213_941_4_FINAL_PRODUCTS 13B-266.sb27263963.eb28581170.56639.31044631945_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131218_946_5_FINAL_PRODUCTS 13B-266.sb27259182.eb28586433.56644.321018194445_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131220_941_5_FINAL_PRODUCTS 13B-266.sb27262884.eb28587751.56646.289167407405_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131221_951_5_FINAL_PRODUCTS 13B-266.sb27243380.eb28590842.56647.268037835645_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131226_946_3_FINAL_PRODUCTS 13B-266.sb27261033.eb28593303.56652.30548047453_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131227_951_5_FINAL_PRODUCTS 13B-266.sb27243380.eb28595272.56653.24096560185_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131229_946_4_FINAL_PRODUCTS 13B-266.sb27260261.eb28597220.56655.44330324074_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131231_941_3_FINAL_PRODUCTS 13B-266.sb27264735.eb28599207.56657.22997615741_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140102_941_3_FINAL_PRODUCTS 13B-266.sb27264735.eb28603800.56659.224627627314_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140105_946_6_FINAL_PRODUCTS 13B-266.sb27258103.eb28612295.56662.34108230324_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140108_951_2_FINAL_PRODUCTS 13B-266.sb25387671.eb28616143.56665.27054978009_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140112_951_1_FINAL_PRODUCTS 13B-266.sb28624226.eb28625769.56669.43262586805_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140115_941_2_FINAL_PRODUCTS 13B-266.sb25390589.eb28636346.56672.1890915625_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131126_946_6_FINAL_PRODUCTS 13B-266.sb27258103.eb28554755.56622.32566427083_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131210_951_6_FINAL_PRODUCTS 13B-266.sb25386827.eb28575527.56636.32553855324_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131217_951_5_FINAL_PRODUCTS 13B-266.sb27243380.eb28586050.56643.330626863426_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131230_941_5_FINAL_PRODUCTS 13B-266.sb27262884.eb28597392.56656.24319659722_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140105_941_3_FINAL_PRODUCTS 13B-266.sb27264735.eb28612294.56662.21643457176_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140106_946_4_1_FINAL_PRODUCTS 13B-266.sb27260261.eb28612383.56663.255257743054_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140106_946_4_2_FINAL_PRODUCTS 13B-266.sb27260261.eb28612384.56663.4214728588_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140116_941_2_FINAL_PRODUCTS 13B-266.sb25390589.eb28660136.56673.2071233912_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140117_946_2_FINAL_PRODUCTS 13B-266.sb25390203.eb28661509.56674.194020532406_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140118_941_2_FINAL_PRODUCTS 13B-266.sb25390589.eb28661624.56675.21209193287_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140119_941_2_FINAL_PRODUCTS 13B-266.sb25390589.eb28661741.56676.19963704861_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140119_946_2_FINAL_PRODUCTS 13B-266.sb25390203.eb28661742.56676.28277457176_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140120_941_2_FINAL_PRODUCTS 13B-266.sb25390589.eb28661773.56677.175470648144_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140121_951_2_1_FINAL_PRODUCTS 13B-266.sb25387671.eb28662252.56678.178276527775_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20140121_951_2_2_FINAL_PRODUCTS 13B-266.sb25387671.eb28662253.56678.261355925926_calibrated_deepfield.ms
tar_ms /mnt/hidata/chiles/final_products/20131222_941_3_FINAL_PRODUCTS 13B-266.sb27264735.eb28591716.56648.26500478009_calibrated_deepfield.ms
