
_now=$(date +"%m_%d_%Y")

_facebook_conversion_data_collect_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/index_collector_conversion_facebook.py"
_facebook_conversion_data_collect_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/data_collectors/facebook_conversion/$_now.log"


python3 $_facebook_conversion_data_collect_py  >> $_facebook_conversion_data_collect_log

