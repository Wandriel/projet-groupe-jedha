#!/usr/bin/env python
"""ETL Pipeline - Point d'entrée unique."""
import argparse
import sys
from etl import Pipeline


def main():
    parser = argparse.ArgumentParser(
        description="ETL Accidents Routiers",
        epilog="Exemples: python main.py etl  /  python main.py rds  /  python main.py full"
    )
    
    parser.add_argument('action', choices=['etl', 'rds', 'full'])
    parser.add_argument('--dry-run', action='store_true', help='Simuler sans écrire')
    
    args = parser.parse_args()
    
    p = Pipeline(dry_run=args.dry_run)
    
    if args.action in ['etl', 'full']:
        p.etl()
    
    if args.action in ['rds', 'full']:
        p.rds()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
