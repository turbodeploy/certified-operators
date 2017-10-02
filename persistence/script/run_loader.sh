
echo "running entity_loader.rb in background..."
nohup $persistence_BASE/script/runner -e production $persistence_BASE/script/entity_loader.rb -q false > $persistence_BASE/log/entity_loader.log &
