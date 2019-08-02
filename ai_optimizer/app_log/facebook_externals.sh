
_now=$(date +"%m_%d_%Y")
_facebook_external_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/facebook_externals.py"
_facebook_external_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/facebook_externals_folder/$_now.log"
python3 $_facebook_external_py  >> $_facebook_external_log
