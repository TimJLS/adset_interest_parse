
_now=$(date +"%m_%d_%Y")
_facebook_campaign_suggestion_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/facebook_campaign_suggestion.py"
_facebook_campaign_suggestion_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/campaign_suggestion/facebook/$_now.log"
python3 $_facebook_campaign_suggestion_py  >> $_facebook_campaign_suggestion_log
