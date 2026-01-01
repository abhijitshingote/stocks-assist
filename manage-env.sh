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
    echo "Usage: $0 [prod|dev] [start|stop [--volumes]|restart|logs|shell|backup|restore|init|update|status] | $0 clean"
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
        echo "Initializing $ENV database..."
        docker-compose -f $COMPOSE_FILE exec $BACKEND_SERVICE bash -c "
          python db_scripts/initialize_data/initialize_db.py --reset && \
          python db_scripts/initialize_data/seed_tickers_from_fmp.py && \
          python db_scripts/initialize_data/seed_earnings_from_fmp.py && \
          python db_scripts/initialize_data/seed_analyst_estimates_from_fmp.py && \
          python db_scripts/initialize_data/seed_index_prices_fmp.py && \
          python db_scripts/initialize_data/seed_index_constituents_fmp.py && \
          python db_scripts/initialize_data/seed_ohlc_from_fmp.py && \
          python db_scripts/initialize_data/seed_profiles_from_fmp.py && \
          python db_scripts/initialize_data/seed_ratios_from_fmp.py && \
          python db_scripts/update_data/stock_metrics_update.py && \
          python db_scripts/update_data/historical_rsi_update.py && \
          python db_scripts/update_data/rsi_indices_update.py && \
          python db_scripts/update_data/volspike_gapper_update.py && \
          python db_scripts/update_data/main_view_update.py && \
          python db_scripts/initialize_data/seed_stock_notes.py && \
          python db_scripts/initialize_data/seed_stock_preferences.py
        "
        ;;
    update)
        echo "Updating $ENV database..."
        docker-compose -f $COMPOSE_FILE exec $BACKEND_SERVICE bash -c "
          python db_scripts/update_data/daily_price_update.py && \
          python db_scripts/update_data/daily_indices_update.py && \
          python db_scripts/update_data/stock_metrics_update.py && \
          python db_scripts/update_data/historical_rsi_update.py && \
          python db_scripts/update_data/rsi_indices_update.py && \
          python db_scripts/update_data/volspike_gapper_update.py
        "
        ;;
    status)
        echo "Status of $ENV environment:"
        docker-compose -f $COMPOSE_FILE ps
        ;;
    *)
        echo "Usage: $0 [prod|dev] [start|stop [--volumes]|restart|logs|shell|backup|restore|init|update|status] | $0 clean"
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
        echo "  $0 clean                         # Clean up unused Docker resources"
        ;;
esac
