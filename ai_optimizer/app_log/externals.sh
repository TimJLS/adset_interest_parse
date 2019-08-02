
_now=$(date +"%m_%d_%Y")
_facebook_external_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/facebook_externals.py"
_facebook_external_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/externals/facebook/$_now.log"

_gdn_external_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/gdn_externals.py" 
_gdn_external_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/externals/gdn/$_now.log"

_gsn_external_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/gsn_externals.py" 
_gsn_external_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/externals/gsn/$_now.log"


python3 $_facebook_external_py  >> $_facebook_external_log
python3 $_gdn_external_py  >> $_gdn_external_log
python3 $_gsn_external_py  >> $_gsn_external_log
