
_now=$(date +"%m_%d_%Y")
_facebook_data_collect_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/facebook_datacollector.py"
_facebook_data_collect_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/data_collectors/facebook/$_now.log"

_facebook_conversion_data_collect_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/index_collector_conversion_facebook.py"
_facebook_conversion_data_collect_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/data_collectors/facebook_conversion/$_now.log"

_facebook_custom_conversion_data_collect_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/facebook_custom_conversion_datacollector.py"
_facebook_custom_conversion_data_collect_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/data_collectors/facebook_conversion/$_now.log"

_facebook_leagen_data_collect_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/index_collector_leadgen_facebook.py"
_facebook_leagen_data_collect_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/data_collectors/facebook_leadgen/$_now.log"

_gdn_data_collect_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/gdn_datacollector.py" 
_gdn_data_collect_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/data_collectors/gdn/$_now.log"

_gsn_data_collect_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/gsn_datacollector.py" 
_gsn_data_collect_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/data_collectors/gsn/$_now.log"

python3 $_facebook_data_collect_py  >> $_facebook_data_collect_log
python3 $_facebook_conversion_data_collect_py  >> $_facebook_conversion_data_collect_log
python3 $_facebook_custom_conversion_data_collect_py  >> $_facebook_custom_conversion_data_collect_log
python3 $_facebook_leagen_data_collect_py  >> $_facebook_leagen_data_collect_log
python3 $_gdn_data_collect_py  >> $_gdn_data_collect_log
python3 $_gsn_data_collect_py  >> $_gsn_data_collect_log
