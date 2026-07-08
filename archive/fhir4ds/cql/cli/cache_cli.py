#!/usr/bin/env python3
"""
CQL Cache Management CLI

Command-line interface for managing CQL terminology caches,
monitoring performance, and debugging cache operations.
"""

import argparse
import sys
import json
import logging
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from fhir4ds.cql.utilities.cache_management import CQLCacheManager
from fhir4ds.cql.core.engine import CQLEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_cache_manager(database_path: Optional[str] = None, dialect: str = "duckdb") -> CQLCacheManager:
    """Set up cache manager with database connection."""
    try:
        if database_path:
            if dialect == "duckdb":
                import duckdb
                conn = duckdb.connect(database_path)
            else:
                import psycopg2
                conn = psycopg2.connect(database_path)
        else:
            # Use in-memory database for demonstration
            if dialect == "duckdb":
                import duckdb
                conn = duckdb.connect(':memory:')
            else:
                conn = None
                
        return CQLCacheManager(db_connection=conn, dialect=dialect)
    except Exception as e:
        logger.error(f"Failed to set up cache manager: {e}")
        sys.exit(1)


def cmd_stats(args):
    """Display cache statistics."""
    print("🔍 CQL Cache Statistics")
    print("=" * 50)
    
    cache_manager = setup_cache_manager(args.database, args.dialect)
    stats = cache_manager.get_cache_statistics()
    
    # Display basic information
    print(f"Database Dialect: {stats['dialect']}")
    print(f"Timestamp: {stats['timestamp']}")
    print()
    
    # Display terminology cache stats
    terminology_stats = stats.get('terminology_cache', {})
    if 'error' in terminology_stats:
        print(f"❌ Error: {terminology_stats['error']}")
        return
    
    if 'tables_found' in terminology_stats:
        tables = terminology_stats['tables_found']
        print(f"📊 Cache Tables Found: {len(tables)}")
        
        if 'table_stats' in terminology_stats:
            total_entries = 0
            total_expired = 0
            
            for table, table_stats in terminology_stats['table_stats'].items():
                if isinstance(table_stats, dict) and 'error' not in table_stats:
                    entries = table_stats.get('total_entries', 0)
                    expired = table_stats.get('expired_entries', 0)
                    active = table_stats.get('active_entries', 0)
                    avg_hits = table_stats.get('average_hits', 0)
                    
                    print(f"\n  📁 {table}:")
                    print(f"    Total Entries: {entries:,}")
                    print(f"    Active Entries: {active:,}")
                    print(f"    Expired Entries: {expired:,}")
                    print(f"    Average Hits: {avg_hits}")
                    
                    total_entries += entries
                    total_expired += expired
            
            print(f"\n📈 Summary:")
            print(f"  Total Cache Entries: {total_entries:,}")
            print(f"  Active Entries: {total_entries - total_expired:,}")
            print(f"  Expired Entries: {total_expired:,}")
            if total_entries > 0:
                expiration_rate = (total_expired / total_entries) * 100
                print(f"  Expiration Rate: {expiration_rate:.1f}%")
    
    # Display performance metrics
    performance = stats.get('performance_metrics', {})
    if performance:
        print(f"\n⚡ Performance Metrics:")
        for key, value in performance.items():
            if key != 'note':
                print(f"  {key.replace('_', ' ').title()}: {value}")
    
    # Display storage usage
    storage = stats.get('storage_usage', {})
    if storage and 'error' not in storage:
        print(f"\n💾 Storage Usage:")
        if 'database_size_mb' in storage:
            print(f"  Database Size: {storage['database_size_mb']} MB")


def cmd_health(args):
    """Display cache health score."""
    print("🏥 CQL Cache Health Check")
    print("=" * 50)
    
    cache_manager = setup_cache_manager(args.database, args.dialect)
    health = cache_manager.get_cache_health_score()
    
    score = health['health_score']
    status = health['status']
    
    # Display health score with color coding
    if score >= 90:
        print(f"✅ Health Score: {score}/100 ({status})")
    elif score >= 75:
        print(f"🟡 Health Score: {score}/100 ({status})")
    elif score >= 50:
        print(f"🟠 Health Score: {score}/100 ({status})")
    else:
        print(f"❌ Health Score: {score}/100 ({status})")
    
    # Display issues
    issues = health.get('issues', [])
    if issues:
        print(f"\n⚠️  Issues Detected:")
        for issue in issues:
            print(f"  • {issue}")
    else:
        print(f"\n✅ No issues detected")
    
    # Display recommendations
    recommendations = health.get('recommendations', [])
    if recommendations:
        print(f"\n💡 Recommendations:")
        for rec in recommendations:
            priority = rec.get('priority', 'medium')
            emoji = '🔴' if priority == 'high' else '🟡' if priority == 'medium' else '🟢'
            print(f"  {emoji} {rec.get('recommendation', 'N/A')}")
            if 'action' in rec:
                print(f"    Action: {rec['action']}")


def cmd_cleanup(args):
    """Clean up expired cache entries."""
    print("🧹 CQL Cache Cleanup")
    print("=" * 50)
    
    cache_manager = setup_cache_manager(args.database, args.dialect)
    
    if args.all:
        print("⚠️  WARNING: This will clear ALL cache entries!")
        if not args.force:
            response = input("Are you sure? (y/N): ")
            if response.lower() != 'y':
                print("Operation cancelled.")
                return
        
        result = cache_manager.clear_all_cache()
        operation = "All cache entries cleared"
    else:
        result = cache_manager.clear_expired_cache()
        operation = "Expired cache entries cleared"
    
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        return
    
    print(f"✅ {operation}")
    print(f"Total entries removed: {result.get('total_removed', 0):,}")
    
    # Show per-table results
    tables_cleaned = result.get('tables_cleaned', {})
    if tables_cleaned:
        print(f"\nPer-table results:")
        for table, table_result in tables_cleaned.items():
            if table_result.get('status') == 'success':
                removed = table_result.get('expired_entries_removed', 0) if not args.all else table_result.get('entries_removed', 0)
                print(f"  📁 {table}: {removed:,} entries removed")
            else:
                print(f"  ❌ {table}: Error - {table_result.get('error', 'Unknown error')}")


def cmd_optimize(args):
    """Optimize cache tables."""
    print("⚡ CQL Cache Optimization")
    print("=" * 50)
    
    cache_manager = setup_cache_manager(args.database, args.dialect)
    result = cache_manager.optimize_cache()
    
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        return
    
    print("✅ Cache optimization completed")
    
    operations = result.get('operations', [])
    if operations:
        print(f"\nOperations performed:")
        for operation in operations:
            print(f"  • {operation}")


def cmd_report(args):
    """Generate comprehensive cache report."""
    print("📊 Generating CQL Cache Report")
    print("=" * 50)
    
    cache_manager = setup_cache_manager(args.database, args.dialect)
    
    try:
        output_file = cache_manager.export_cache_report(args.output)
        print(f"✅ Cache report generated: {output_file}")
        
        # Display summary if not in quiet mode
        if not args.quiet:
            with open(output_file, 'r') as f:
                report = json.load(f)
            
            stats = report.get('cache_statistics', {})
            terminology_stats = stats.get('terminology_cache', {})
            
            if 'table_stats' in terminology_stats:
                total_entries = sum(
                    table_data.get('total_entries', 0) 
                    for table_data in terminology_stats['table_stats'].values()
                    if isinstance(table_data, dict) and 'total_entries' in table_data
                )
                print(f"\nReport Summary:")
                print(f"  Total cache entries: {total_entries:,}")
                print(f"  Report size: {Path(output_file).stat().st_size:,} bytes")
                print(f"  Generated at: {report['report_metadata']['generated_at']}")
        
    except Exception as e:
        print(f"❌ Error generating report: {e}")


def cmd_monitor(args):
    """Monitor cache in real-time."""
    import time
    
    print("👁️  CQL Cache Monitor")
    print("=" * 50)
    print("Press Ctrl+C to stop monitoring")
    print()
    
    cache_manager = setup_cache_manager(args.database, args.dialect)
    
    try:
        while True:
            # Clear screen
            if hasattr(args, 'clear') and args.clear:
                import os
                os.system('clear' if os.name == 'posix' else 'cls')
            
            # Get current stats
            stats = cache_manager.get_cache_statistics()
            health = cache_manager.get_cache_health_score()
            
            print(f"🕐 {stats['timestamp']}")
            print(f"🏥 Health: {health['health_score']}/100 ({health['status']})")
            
            # Show cache summary
            terminology_stats = stats.get('terminology_cache', {})
            if 'table_stats' in terminology_stats:
                total_entries = sum(
                    table_data.get('total_entries', 0) 
                    for table_data in terminology_stats['table_stats'].values()
                    if isinstance(table_data, dict) and 'total_entries' in table_data
                )
                total_expired = sum(
                    table_data.get('expired_entries', 0) 
                    for table_data in terminology_stats['table_stats'].values()
                    if isinstance(table_data, dict) and 'expired_entries' in table_data
                )
                
                print(f"📊 Entries: {total_entries:,} total, {total_expired:,} expired")
            
            print(f"\n(Refreshing every {args.interval} seconds...)")
            print("-" * 50)
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="CQL Cache Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s stats                    # Show cache statistics
  %(prog)s health                   # Check cache health
  %(prog)s cleanup                  # Clean expired entries
  %(prog)s cleanup --all --force    # Clear all cache entries
  %(prog)s optimize                 # Optimize cache tables
  %(prog)s report -o cache_report.json  # Generate detailed report
  %(prog)s monitor                  # Real-time monitoring
        """
    )
    
    # Global arguments
    parser.add_argument('--database', '-d', 
                       help='Database connection string or path')
    parser.add_argument('--dialect', 
                       choices=['duckdb', 'postgresql'], 
                       default='duckdb',
                       help='Database dialect (default: duckdb)')
    parser.add_argument('--verbose', '-v', 
                       action='store_true',
                       help='Verbose output')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show cache statistics')
    
    # Health command
    health_parser = subparsers.add_parser('health', help='Check cache health')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean cache entries')
    cleanup_parser.add_argument('--all', action='store_true',
                               help='Clear all entries (not just expired)')
    cleanup_parser.add_argument('--force', action='store_true',
                               help='Force operation without confirmation')
    
    # Optimize command
    optimize_parser = subparsers.add_parser('optimize', help='Optimize cache tables')
    
    # Report command
    report_parser = subparsers.add_parser('report', help='Generate cache report')
    report_parser.add_argument('--output', '-o',
                              help='Output file path')
    report_parser.add_argument('--quiet', '-q', action='store_true',
                              help='Quiet mode - no summary output')
    
    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor cache in real-time')
    monitor_parser.add_argument('--interval', '-i', type=int, default=5,
                               help='Refresh interval in seconds (default: 5)')
    monitor_parser.add_argument('--clear', action='store_true',
                               help='Clear screen on each refresh')
    
    args = parser.parse_args()
    
    # Set up logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Route to appropriate command
    if args.command == 'stats':
        cmd_stats(args)
    elif args.command == 'health':
        cmd_health(args)
    elif args.command == 'cleanup':
        cmd_cleanup(args)
    elif args.command == 'optimize':
        cmd_optimize(args)
    elif args.command == 'report':
        cmd_report(args)
    elif args.command == 'monitor':
        cmd_monitor(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()