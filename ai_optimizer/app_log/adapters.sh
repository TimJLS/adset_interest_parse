
_now=$(date +"%m_%d_%Y")
_facebook_adapter_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/facebook_adapter.py"
_facebook_adapter_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/adapters/facebook/$_now.log"

_gdn_adapter_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/gdn_adapter.py" 
_gdn_adapter_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/adapters/gdn/$_now.log"

_gsn_adapter_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/gsn_adapter.py" 
_gsn_adapter_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/adapters/gsn/$_now.log"


python3 $_facebook_adapter_py  >> $_facebook_adapter_log
python3 $_gdn_adapter_py  >> $_gdn_adapter_log
python3 $_gsn_adapter_py  >> $_gsn_adapter_log
