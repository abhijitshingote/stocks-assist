#!/bin/bash

# Environment management script for stocks-assist
# Usage: ./manage-env.sh [prod|dev] [start|stop [--volumes]|restart|logs|shell|backup|restore]

if [ "$1" = "clean" ]; then
    ACTION="clean"
    ENV=""
else
    ENV=${1:-prod}
    ACTION=${2:-start}
fi

COMPOSE_FILE=""
if [ "$ENV" = "prod" ]; then
    COMPOSE_FILE="docker-compose.yml"
    DB_SERVICE="db"
    BACKEND_SERVICE="backend"
    FRONTEND_SERVICE="frontend"
    DB_NAME="stocks_db"
    DB_PORT="5432"
    BACKEND_PORT="5001"
    FRONTEND_PORT="5002"
elif [ "$ENV" = "dev" ]; then
    COMPOSE_FILE="docker-compose.dev.yml"
    DB_SERVICE="db-dev"
    BACKEND_SERVICE="backend-dev"
    FRONTEND_SERVICE="frontend-dev"
    DB_NAME="stocks_db_dev"
    DB_PORT="5433"
    BACKEND_PORT="5003"
    FRONTEND_PORT="5004"
elif [ -n "$ENV" ]; then
    echo "Usage: $0 [prod|dev] [start|stop [--volumes]|restart|logs|shell|backup|restore|init|update|status|list-logs] | $0 clean"
    exit 1
fi

case $ACTION in
    clean)
        echo "Cleaning up Docker resources..."
        docker system prune -f
        echo "Cleanup complete."
        exit 0
        ;;
    start)
        echo "Starting $ENV environment..."
        docker-compose -f $COMPOSE_FILE up -d
        echo "$ENV environment started:"
        echo "  Frontend: http://localhost:$FRONTEND_PORT"
        echo "  Backend: http://localhost:$BACKEND_PORT"
        echo "  Database: localhost:$DB_PORT"
        ;;
    stop)
        VOLUMES_FLAG=""
        if [ "$3" = "--volumes" ]; then
            VOLUMES_FLAG="--volumes"
            echo "Stopping $ENV environment and removing volumes..."
        else
            echo "Stopping $ENV environment..."
        fi
        if ! docker-compose -f $COMPOSE_FILE down $VOLUMES_FLAG 2>/dev/null; then
            echo "Warning: Some resources may not have been cleaned up properly."
            echo "You can manually clean up with: docker system prune"
        fi
        ;;
    restart)
        echo "Restarting $ENV environment..."
        docker-compose -f $COMPOSE_FILE restart
        ;;
    logs)
        SERVICE=${3:-all}
        if [ "$SERVICE" = "all" ]; then
            docker-compose -f $COMPOSE_FILE logs -f
        else
            docker-compose -f $COMPOSE_FILE logs -f $SERVICE
        fi
        ;;
    shell)
        SERVICE=${3:-backend}
        if [ "$SERVICE" = "db" ]; then
            docker-compose -f $COMPOSE_FILE exec $DB_SERVICE psql -U postgres -d $DB_NAME
        else
            docker-compose -f $COMPOSE_FILE exec $SERVICE bash
        fi
        ;;
    backup)
        echo "Creating $ENV database backup..."
        TIMESTAMP=$(date +%Y%m%d_%H%M%S)
        BACKUP_FILE="stocks_db_backup_${ENV}_${TIMESTAMP}.dump"
        docker-compose -f $COMPOSE_FILE exec -T $DB_SERVICE pg_dump -U postgres -Fc $DB_NAME > $BACKUP_FILE
        echo "Backup saved as: $BACKUP_FILE"
        ;;
    restore)
        BACKUP_FILE=${3}
        if [ -z "$BACKUP_FILE" ]; then
            echo "Usage: $0 $ENV restore <backup_file>"
            exit 1
        fi
        if [ ! -f "$BACKUP_FILE" ]; then
            echo "Backup file not found: $BACKUP_FILE"
            exit 1
        fi
        echo "Restoring $ENV database from $BACKUP_FILE..."
        docker-compose -f $COMPOSE_FILE exec -T $DB_SERVICE pg_restore -U postgres -d $DB_NAME --clean --if-exists < $BACKUP_FILE
        ;;
   init)
    # Parse additional flags
    TEST_MODE=""
    TEST_LIMIT=10
    for arg in "${@:3}"; do
        case $arg in
            --test)
                TEST_MODE="true"
                ;;
            --test=*)
                TEST_MODE="true"
                TEST_LIMIT="${arg#*=}"
                ;;
        esac
    done

    # Setup logging
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    LOG_FILE="logs/${ENV}_init_${TIMESTAMP}.log"
    mkdir -p logs

    if [ "$TEST_MODE" = "true" ]; then
        echo "🧪 TEST MODE: Limiting to $TEST_LIMIT tickers" >> $LOG_FILE
    fi

    echo "Initializing $ENV database..."
    echo "Log file: $LOG_FILE"

    scripts=(
        "db_scripts/initialize_data/initialize_db.py --reset:Reset database tables"
        "db_scripts/initialize_data/seed_tickers_from_fmp.py:Seed tickers from FMP screener"
        "db_scripts/initialize_data/seed_earnings_from_fmp.py:Seed earnings data"
        "db_scripts/initialize_data/seed_analyst_estimates_from_fmp.py:Seed analyst estimates"
        "db_scripts/initialize_data/seed_index_prices_fmp.py:Seed index/ETF prices"
        "db_scripts/initialize_data/seed_index_constituents_fmp.py:Seed index constituents"
        "db_scripts/initialize_data/seed_ohlc_from_fmp.py:Seed OHLC price history"
        "db_scripts/initialize_data/seed_profiles_from_fmp.py:Seed company profiles"
        "db_scripts/initialize_data/seed_ratios_from_fmp.py:Seed financial ratios"
        "db_scripts/initialize_data/seed_shares_float_from_fmp.py:Seed shares float data"
        "db_scripts/update_data/stock_metrics_update.py:Compute stock metrics"
        "db_scripts/update_data/historical_rsi_update.py:Compute historical RSI"
        "db_scripts/update_data/rsi_indices_update.py:Compute RSI for indices"
        "db_scripts/update_data/volspike_gapper_update.py:Detect volume spikes/gappers"
        "db_scripts/update_data/main_view_update.py:Update main screener view"
        "db_scripts/initialize_data/seed_stock_notes.py:Seed user stock notes"
        "db_scripts/initialize_data/seed_stock_preferences.py:Seed user stock preferences"
    )

    total_scripts=${#scripts[@]}
    completed=0
    start_time=$(date +%s)

    for script_info in "${scripts[@]}"; do
        script=$(echo "$script_info" | cut -d':' -f1)
        description=$(echo "$script_info" | cut -d':' -f2-)
        completed=$((completed + 1))

        # Write header to log only
        {
            echo ""
            echo "[$completed/$total_scripts] Running: $description"
            echo "Script: $script"
            echo "Started at: $(date '+%H:%M:%S')"
        } >> $LOG_FILE

        script_start=$(date +%s)

        # Build command
        if [ "$TEST_MODE" = "true" ]; then
            CMD="TEST_TICKER_LIMIT=$TEST_LIMIT python $script"
        else
            CMD="python $script"
        fi

        # Run in Docker, disable TTY, append output only to log
        if docker-compose -f $COMPOSE_FILE exec -T -e TEST_TICKER_LIMIT=${TEST_MODE:+$TEST_LIMIT} $BACKEND_SERVICE sh -c "$CMD" >> $LOG_FILE 2>&1; then
            script_end=$(date +%s)
            duration=$((script_end - script_start))
            echo "✅ Completed in ${duration}s" >> $LOG_FILE
        else
            echo "❌ Failed: $script" >> $LOG_FILE
            echo "Database initialization stopped due to error." >> $LOG_FILE
            exit 1
        fi
    done

    end_time=$(date +%s)
    total_duration=$((end_time - start_time))
    {
        echo ""
        echo "=================================="
        echo "✅ Database initialization completed!"
        echo "Total time: ${total_duration}s"
        echo "=================================="
    } >> $LOG_FILE
    ;;
    update)
        # Parse additional flags
        TEST_MODE=""
        TEST_LIMIT=10
        for arg in "${@:3}"; do
            case $arg in
                --test)
                    TEST_MODE="true"
                    ;;
                --test=*)
                    TEST_MODE="true"
                    TEST_LIMIT="${arg#*=}"
                    ;;
            esac
        done

        # Setup logging
        TIMESTAMP=$(date +%Y%m%d_%H%M%S)
        LOG_FILE="logs/${ENV}_update_${TIMESTAMP}.log"
        mkdir -p logs

        if [ "$TEST_MODE" = "true" ]; then
            echo "🧪 TEST MODE: Limiting to $TEST_LIMIT tickers"
            echo "🧪 TEST MODE: Limiting to $TEST_LIMIT tickers" >> $LOG_FILE
        fi

        echo "Updating $ENV database..."
        echo "Log file: $LOG_FILE"
        echo ""
        echo "Database update scripts:"
        echo "========================"

        # Also write to log file
        {
            echo "Database update scripts:"
            echo "========================"
        } >> $LOG_FILE

        scripts=(
            "db_scripts/update_data/daily_price_update.py:Update daily stock prices"
            "db_scripts/update_data/daily_indices_update.py:Update daily index prices"
            "db_scripts/update_data/stock_metrics_update.py:Update stock metrics"
            "db_scripts/update_data/historical_rsi_update.py:Update historical RSI"
            "db_scripts/update_data/rsi_indices_update.py:Update RSI for indices"
            "db_scripts/update_data/volspike_gapper_update.py:Update volume spikes/gappers"
            "db_scripts/update_data/main_view_update.py:Update main screener view"
        )

        total_scripts=${#scripts[@]}
        completed=0
        start_time=$(date +%s)

        for script_info in "${scripts[@]}"; do
            script=$(echo "$script_info" | cut -d':' -f1)
            description=$(echo "$script_info" | cut -d':' -f2-)
            completed=$((completed + 1))

            {
                echo ""
                echo "[$completed/$total_scripts] Running: $description"
                echo "Script: $script"
                echo "Started at: $(date '+%H:%M:%S')"
            } | tee -a $LOG_FILE

            script_start=$(date +%s)

            # Build command with test mode env var if enabled
            if [ "$TEST_MODE" = "true" ]; then
                CMD="TEST_TICKER_LIMIT=$TEST_LIMIT python $script"
            else
                CMD="python $script"
            fi

            if docker-compose -f $COMPOSE_FILE exec -e TEST_TICKER_LIMIT=${TEST_MODE:+$TEST_LIMIT} $BACKEND_SERVICE sh -c "$CMD" 2>&1 | tee -a $LOG_FILE; then
                script_end=$(date +%s)
                duration=$((script_end - script_start))
                echo "✅ Completed in ${duration}s" | tee -a $LOG_FILE
            else
                echo "❌ Failed: $script" | tee -a $LOG_FILE
                echo "Database update stopped due to error." | tee -a $LOG_FILE
                exit 1
            fi
        done

        end_time=$(date +%s)
        total_duration=$((end_time - start_time))
        {
            echo ""
            echo "========================"
            echo "✅ Database update completed!"
            echo "Total time: ${total_duration}s"
            echo "========================"
        } | tee -a $LOG_FILE
        ;;
    status)
        echo "Status of $ENV environment:"
        docker-compose -f $COMPOSE_FILE ps
        ;;
    list-logs)
        echo "Available log files for $ENV:"
        echo "=============================="
        ls -la logs/${ENV}_*.log 2>/dev/null || echo "No log files found for $ENV environment."
        ;;
    *)
        echo "Usage: $0 [prod|dev] [start|stop [--volumes]|restart|logs|shell|backup|restore|init|update|status|list-logs] | $0 clean"
        echo ""
        echo "Examples:"
        echo "  $0 prod start                    # Start production environment"
        echo "  $0 dev start                     # Start development environment"
        echo "  $0 prod stop --volumes           # Stop production and remove data volumes"
        echo "  $0 dev stop                      # Stop development (keep data)"
        echo "  $0 prod logs backend             # View backend logs"
        echo "  $0 dev shell db                  # Open database shell"
        echo "  $0 prod backup                   # Create production backup"
        echo "  $0 dev restore backup_file.dump  # Restore dev from backup"
        echo "  $0 prod init                     # Initialize production database"
        echo "  $0 dev update                    # Update dev database"
        echo "  $0 prod init --test              # Initialize with 10 test tickers"
        echo "  $0 dev update --test=5           # Update with 5 test tickers"
        echo "  $0 prod list-logs                # List available log files"
        echo "  $0 clean                         # Clean up unused Docker resources"
        ;;
esac
