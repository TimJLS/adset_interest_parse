_now=$(date +"%m_%d_%Y")
_facebook_creative_controller_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/facebook_creative_controller.py"
_facebook_creative_controller_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/creative_controller/facebook/$_now.log"
python3 $_facebook_creative_controller_py  >> $_facebook_creative_controller_log
