
_now=$(date +"%m_%d_%Y")
_facebook_smart_spending_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/facebook_smart_spending.py"
_facebook_smart_spending_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/smart_spending/facebook/$_now.log"
python3 $_facebook_smart_spending_py  >> $_facebook_smart_spending_log
