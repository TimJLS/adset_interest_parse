
_now=$(date +"%m_%d_%Y")
_facebook_genetic_algorithm_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/genetic_algorithm.py"
_facebook_genetic_algorithm_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/genetic_algorithms/facebook/$_now.log"

_gdn_genetic_algorithm_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/genetic_algorithm_gdn.py" 
_gdn_genetic_algorithm_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/genetic_algorithms/gdn/$_now.log"

_gsn_genetic_algorithm_py="/home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/genetic_algorithm_gsn.py" 
_gsn_genetic_algorithm_log="/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/genetic_algorithms/gsn/$_now.log"


python3 $_facebook_genetic_algorithm_py  >> $_facebook_genetic_algorithm_log
python3 $_gdn_genetic_algorithm_py  >> $_gdn_genetic_algorithm_log
python3 $_gsn_genetic_algorithm_py  >> $_gsn_genetic_algorithm_log
