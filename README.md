
# Customer Segmentation Module 1.2
This module try to denote one of five labels (Bargain hunter, Smart shopper, Finest focus, Pleasure focus and Quick shopper) to customers. 

Note: Not every customer should have a label after excution


## user guide
1.  When we need to train model , and process the data for the dashboard .
    - set the ```config``` inside of the controller.py, 
    - then run ```python3.6  controller.py```
2.  When we only need to process the lastest data for the dashboard.
    - set the ```config``` inside of the customer_lifestyle_segmetation_data_processor_for_dashboard.py, 
    - then run ```python3.6  customer_lifestyle_segmetation_data_processor_for_dashboard.py```



## This folder contain 11 files as below:

  - controller.py
  - step3_seg_item_detail_1.py
  - step4_seg_item_detail_2.py
  - step5_seg_finest_amplitude_word2vec.py
  - step6_seg_pleasure_score_word2vec.py
  - step7_set_customers_value.py
  - step8_seg_cutoff_method.py
  - customer_lifestyle_segmetation_data_processor_for_dashboard.py
  - data/
  - seg_table_create.sql
  - README.md

### controller.py

purpose : A launcher for Module 1.2, we can use it to trigger all steps's functions from other file, and generate the result from the raw data automatically.

usage: 

  - set the ```config dictionary``` including 5 variables in the controller.py
  - run ```python3.6  controller.py```

### step3_seg_item_detail_1.py.py

contains : 4.6 from the document

### step4_seg_item_detail_2.py.py

contains : 4.6 from the document

### step5_seg_finest_amplitude_word2vec.py.py

contains : 4.7 from the document

### step6_seg_pleasure_score_word2vec.py.py

contains : 4.7 from the document

### step7_set_customers_value.py.py

contains : 5.1 from the document

### step8_seg_cutoff_method.py.py

contains : 5.2 from the document

### customer_lifestyle_segmetation_data_processor_for_dashboard.py
purpose : process the result and insert into Yonghong dashboard

two ways to use it: 

1.  trigger by controller.py
2.  set the ```config``` in the customer_lifestyle_segmetation_data_processor_for_dashboard.py, run ```python3.6  customer_lifestyle_segmetation_data_processor_for_dashboard.py```

### data/
purpose : temp data folder for placing the model's input, the path can be set in the config of controller.py


### seg_table_create.sql
purpose : display the SQLs used in controller.py, in the sql format.
