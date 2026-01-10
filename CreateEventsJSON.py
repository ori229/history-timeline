#!/usr/bin/env python3
"""
Wikipedia Article Data Collector
Collects start/end dates and English pageviews for Hebrew Wikipedia articles
"""

import json
import requests
import time
from datetime import datetime
from typing import Optional, Dict, List
import sys

class WikipediaCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WikipediaCollector/1.0 (Educational purposes)'
        })
    
    def get_wikidata_id(self, hebrew_title: str) -> Optional[str]:
        """Get Wikidata ID from Hebrew Wikipedia article title"""
        url = "https://he.wikipedia.org/w/api.php"
        params = {
            'action': 'query',
            'titles': hebrew_title,
            'prop': 'pageprops',
            'format': 'json',
            'ppprop': 'wikibase_item'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            pages = data.get('query', {}).get('pages', {})
            for page_id, page_data in pages.items():
                if page_id != '-1':  # Article exists
                    return page_data.get('pageprops', {}).get('wikibase_item')
        except Exception as e:
            print(f"Error getting Wikidata ID for {hebrew_title}: {e}", file=sys.stderr)
        
        return None
    
    def get_wikidata_info(self, wikidata_id: str) -> Dict:
        """Get start date, end date, and English article title from Wikidata"""
        url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            entity = data.get('entities', {}).get(wikidata_id, {})
            claims = entity.get('claims', {})
            sitelinks = entity.get('sitelinks', {})
            
            # Get English article title
            english_title = sitelinks.get('enwiki', {}).get('title')
            
            # Get start date (P580 - start time)
            start_date = self._extract_date(claims.get('P580', []))
            
            # Get end date (P582 - end time)
            end_date = self._extract_date(claims.get('P582', []))
            
            # If no start/end time, try point in time (P585)
            if not start_date and not end_date:
                point_in_time = self._extract_date(claims.get('P585', []))
                if point_in_time:
                    start_date = point_in_time
                    end_date = point_in_time
            
            # Try inception (P571) for start if still not found
            if not start_date:
                start_date = self._extract_date(claims.get('P571', []))
            
            # Try dissolution/abolished date (P576) for end if still not found
            if not end_date:
                end_date = self._extract_date(claims.get('P576', []))
            
            return {
                'english_title': english_title,
                'start_date': start_date,
                'end_date': end_date
            }
        
        except Exception as e:
            print(f"Error getting Wikidata info for {wikidata_id}: {e}", file=sys.stderr)
            return {'english_title': None, 'start_date': None, 'end_date': None}
    
    def _extract_date(self, claims: List) -> Optional[str]:
        """Extract date from Wikidata claims"""
        if not claims:
            return None
        
        try:
            # Take the first claim
            date_value = claims[0].get('mainsnak', {}).get('datavalue', {}).get('value', {})
            
            if isinstance(date_value, dict) and 'time' in date_value:
                # Wikidata time format: +YYYY-MM-DDT00:00:00Z
                time_str = date_value['time']
                precision = date_value.get('precision', 11)  # 11 = day, 10 = month, 9 = year
                
                # Remove the leading + and timezone
                time_str = time_str.lstrip('+').split('T')[0]
                
                # Handle different precisions
                if precision == 9:  # Year precision
                    year = time_str.split('-')[0]
                    return f"{year}-01-01"
                elif precision == 10:  # Month precision
                    parts = time_str.split('-')
                    return f"{parts[0]}-{parts[1]}-01"
                else:  # Day precision or better
                    return time_str
        
        except Exception as e:
            print(f"Error extracting date: {e}", file=sys.stderr)
        
        return None
    
    def get_english_pageviews(self, english_title: str, year: int = 2025) -> int:
        """Get total pageviews for English Wikipedia article in a given year"""
        # For 2025, we can only get data up to current date
        current_date = datetime.now()
        
        if year == 2025:
            # Get from Jan 1, 2025 to yesterday (or Jan 9, 2025 since today is Jan 10)
            start_date = "20250101"
            # Use yesterday's date
            end_date = "20250109"
        else:
            start_date = f"{year}0101"
            end_date = f"{year}1231"
        
        # Wikipedia Pageviews API
        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{english_title.replace(' ', '_')}/daily/{start_date}/{end_date}"
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            total_views = sum(item.get('views', 0) for item in data.get('items', []))
            return total_views
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Pageviews not found for {english_title}", file=sys.stderr)
            else:
                print(f"Error getting pageviews for {english_title}: {e}", file=sys.stderr)
            return 0
        except Exception as e:
            print(f"Error getting pageviews for {english_title}: {e}", file=sys.stderr)
            return 0
    
    def process_article(self, hebrew_title: str) -> Optional[Dict]:
        """Process a single Hebrew Wikipedia article"""
        print(f"Processing: {hebrew_title}", file=sys.stderr)
        
        # Get Wikidata ID
        wikidata_id = self.get_wikidata_id(hebrew_title)
        if not wikidata_id:
            print(f"  No Wikidata ID found for {hebrew_title}", file=sys.stderr)
            return None
        
        print(f"  Wikidata ID: {wikidata_id}", file=sys.stderr)
        
        # Get info from Wikidata
        info = self.get_wikidata_info(wikidata_id)
        
        if not info['english_title']:
            print(f"  No English article found for {hebrew_title}", file=sys.stderr)
            return None
        
        print(f"  English title: {info['english_title']}", file=sys.stderr)
        
        # Get pageviews
        pageviews = self.get_english_pageviews(info['english_title'], 2025)
        print(f"  Pageviews: {pageviews}", file=sys.stderr)
        
        result = {
            'hebrew_article': hebrew_title,
            'start_date': info['start_date'],
            'end_date': info['end_date'],
            'english_article': info['english_title'],
            'english_pageviews_2025': pageviews
        }
        
        # Small delay to be respectful to APIs
        time.sleep(0.5)
        
        return result
    
    def process_file(self, input_file: str, output_file: str = 'output.json'):
        """Process all articles from input file"""
        results = []
        
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                titles = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"Error: Input file '{input_file}' not found", file=sys.stderr)
            return
        except Exception as e:
            print(f"Error reading input file: {e}", file=sys.stderr)
            return
        
        print(f"Found {len(titles)} articles to process\n", file=sys.stderr)
        
        for i, title in enumerate(titles, 1):
            print(f"[{i}/{len(titles)}] ", file=sys.stderr, end='')
            result = self.process_article(title)
            if result:
                results.append(result)
            print('', file=sys.stderr)  # Empty line for readability
        
        # Save results
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"\nResults saved to {output_file}", file=sys.stderr)
            print(f"Successfully processed {len(results)}/{len(titles)} articles", file=sys.stderr)
        except Exception as e:
            print(f"Error writing output file: {e}", file=sys.stderr)

def main():
    if len(sys.argv) < 2:
        print("Usage: python wikipedia_collector.py <input_file> [output_file]")
        print("\nExample:")
        print("  python wikipedia_collector.py input.txt")
        print("  python wikipedia_collector.py input.txt output.json")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'output.json'
    
    collector = WikipediaCollector()
    collector.process_file(input_file, output_file)

if __name__ == '__main__':
    main()