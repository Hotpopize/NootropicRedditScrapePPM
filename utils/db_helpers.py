from core.database import get_db_session, CollectedData, CodedData, Codebook, AuditLog, ReplicabilityLog, ZoteroReference, ZoteroCollectionLink, ScrapeRun
from datetime import datetime
import json

def create_scrape_run(job_id, config_hash, parameters, session_id=None):
    db = get_db_session()
    try:
        run = ScrapeRun(
            job_id=job_id,
            status='RUNNING',
            config_hash=config_hash,
            parameters=parameters,
            session_id=session_id
        )
        db.add(run)
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def update_scrape_run(job_id, status=None, items_collected=None, error_message=None):
    db = get_db_session()
    try:
        run = db.query(ScrapeRun).filter_by(job_id=job_id).first()
        if run:
            if status:
                run.status = status
                if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    run.completed_at = datetime.utcnow()
            if items_collected is not None:
                run.items_collected = items_collected
            if error_message:
                run.error_message = error_message
            db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def save_collected_data(items, session_id):
    db = get_db_session()
    try:
        saved_count = 0
        for item in items:
            existing = db.query(CollectedData).filter_by(reddit_id=item.get('id')).first()
            if not existing:
                record = CollectedData(
                    reddit_id=item.get('id'),
                    type=item.get('type'),
                    subreddit=item.get('subreddit'),
                    title=item.get('title'),
                    text=item.get('text'),
                    author=item.get('author'),
                    score=item.get('score', 0),
                    created_utc=item.get('created_utc'),
                    num_comments=item.get('num_comments'),
                    url=item.get('url'),
                    permalink=item.get('permalink'),
                    post_id=item.get('post_id'),
                    session_id=session_id,
                    extra_metadata=item.get('metadata', {}),
                    data_source=item.get('data_source', 'praw')
                )
                db.add(record)
                saved_count += 1
        
        db.commit()
        return saved_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def load_collected_data(session_id=None, subreddit=None, limit=None):
    db = get_db_session()
    try:
        query = db.query(CollectedData)
        
        if session_id:
            query = query.filter_by(session_id=session_id)
        
        if subreddit:
            query = query.filter_by(subreddit=subreddit)
        
        query = query.order_by(CollectedData.collected_at.desc())
        
        if limit:
            query = query.limit(limit)
        
        results = query.all()
        
        return [
            {
                'id': r.reddit_id,
                'type': r.type,
                'subreddit': r.subreddit,
                'title': r.title,
                'text': r.text,
                'author': r.author,
                'score': r.score,
                'created_utc': r.created_utc,
                'num_comments': r.num_comments,
                'url': r.url,
                'permalink': r.permalink,
                'post_id': r.post_id,
                'data_source': r.data_source or 'praw',
                'collected_at': r.collected_at.isoformat() if r.collected_at else None,
                'metadata': r.extra_metadata or {}
            }
            for r in results
        ]
    finally:
        db.close()

def get_all_collected_reddit_ids():
    db = get_db_session()
    try:
        results = db.query(CollectedData).filter_by(type='submission').all()
        return ['t3_' + r.reddit_id for r in results]
    finally:
        db.close()

def delete_collected_data_by_ids(reddit_ids):
    db = get_db_session()
    try:
        deleted_count = db.query(CollectedData).filter(
            CollectedData.reddit_id.in_(reddit_ids)
        ).delete(synchronize_session=False)
        db.commit()
        return deleted_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def save_coded_data(items, session_id):
    db = get_db_session()
    try:
        saved_count = 0
        for item in items:
            existing = db.query(CodedData).filter_by(
                reddit_id=item.get('id')
            ).first()
            
            if existing:
                existing.ppm_category = item.get('ppm_category')
                existing.ppm_subcodes = item.get('ppm_subcodes', [])
                existing.themes = item.get('themes', [])
                existing.evidence_quotes = item.get('evidence_quotes', [])
                existing.confidence = item.get('confidence')
                existing.coded_at = datetime.utcnow()
                existing.coded_by = item.get('coded_by')
                existing.coding_approach = item.get('coding_approach')
                existing.rationale = item.get('rationale')
                existing.extra_metadata = item.get('metadata', {})
                existing.session_id = session_id
            else:
                record = CodedData(
                    reddit_id=item.get('id'),
                    ppm_category=item.get('ppm_category'),
                    ppm_subcodes=item.get('ppm_subcodes', []),
                    themes=item.get('themes', []),
                    evidence_quotes=item.get('evidence_quotes', []),
                    confidence=item.get('confidence'),
                    coded_by=item.get('coded_by'),
                    coding_approach=item.get('coding_approach'),
                    session_id=session_id,
                    rationale=item.get('rationale'),
                    extra_metadata=item.get('metadata', {})
                )
                db.add(record)
            
            saved_count += 1
        
        db.commit()
        return saved_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def load_coded_data(session_id=None, limit=None):
    db = get_db_session()
    try:
        query = db.query(CodedData)
        
        if session_id:
            query = query.filter_by(session_id=session_id)
        
        query = query.order_by(CodedData.coded_at.desc())
        
        if limit:
            query = query.limit(limit)
        
        results = query.all()
        
        return [
            {
                'id': r.reddit_id,
                'ppm_category': r.ppm_category,
                'ppm_subcodes': r.ppm_subcodes or [],
                'themes': r.themes or [],
                'evidence_quotes': r.evidence_quotes or [],
                'confidence': r.confidence,
                'coded_at': r.coded_at.isoformat() if r.coded_at else None,
                'coded_by': r.coded_by,
                'coding_approach': r.coding_approach,
                'rationale': r.rationale,
                'metadata': r.extra_metadata or {}
            }
            for r in results
        ]
    finally:
        db.close()

def save_codebook(codebook_data, session_id):
    db = get_db_session()
    try:
        current_codes = set()
        # Handle new format {'codes': [...]}
        codes_list = codebook_data.get('codes', []) if isinstance(codebook_data, dict) else []
        
        for code in codes_list:
            category = code.get('category')
            name = code.get('name')
            current_codes.add((category, name))
            
            existing = db.query(Codebook).filter_by(
                category=category,
                name=name
            ).first()
            
            # Pack extra fields into metadata
            meta = {
                'id': code.get('id'),
                'include': code.get('include'),
                'exclude': code.get('exclude'),
                'source': code.get('source'),
                'is_emergent_candidate': code.get('is_emergent_candidate'),
                'created_at': code.get('created_at')
            }
            
            if existing:
                existing.definition = code.get('definition')
                existing.examples = code.get('examples')
                existing.frequency = code.get('frequency', 0)
                existing.extra_metadata = meta
                existing.session_id = session_id
            else:
                record = Codebook(
                    category=category,
                    name=name,
                    definition=code.get('definition'),
                    examples=code.get('examples'),
                    frequency=code.get('frequency', 0),
                    session_id=session_id,
                    extra_metadata=meta
                )
                db.add(record)
        
        # Prune removed codes (Global prune as per original logic)
        all_db_codes = db.query(Codebook).all()
        for db_code in all_db_codes:
            if (db_code.category, db_code.name) not in current_codes:
                db.delete(db_code)
        
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def load_codebook(session_id=None):
    db = get_db_session()
    try:
        if session_id:
            results = db.query(Codebook).filter_by(session_id=session_id).all()
        else:
            results = db.query(Codebook).all()
            
        codes_list = []
        seen_codes = {}
        for r in results:
            key = (r.category, r.name)
            if key in seen_codes:
                continue
            
            seen_codes[key] = True
            
            meta = r.extra_metadata or {}
            
            code_data = {
                'id': meta.get('id') or f"{r.category}-{r.name}",
                'category': r.category,
                'name': r.name,
                'definition': r.definition,
                'examples': r.examples,
                'frequency': r.frequency,
                'include': meta.get('include', ''),
                'exclude': meta.get('exclude', ''),
                'source': meta.get('source', ''),
                'is_emergent_candidate': meta.get('is_emergent_candidate', False),
                'created_at': meta.get('created_at') or (r.added_at.isoformat() if r.added_at else None)
            }
            codes_list.append(code_data)
        
        return {'codes': codes_list}
    finally:
        db.close()

def log_action(action, session_id, details):
    db = get_db_session()
    try:
        log = AuditLog(
            action=action,
            session_id=session_id,
            details=details
        )
        db.add(log)
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def load_audit_logs(session_id, action_filter=None, limit=100):
    db = get_db_session()
    try:
        query = db.query(AuditLog).filter_by(session_id=session_id)
        
        if action_filter:
            query = query.filter_by(action=action_filter)
        
        query = query.order_by(AuditLog.timestamp.desc()).limit(limit)
        
        results = query.all()
        
        return [
            {
                'timestamp': r.timestamp.isoformat() if r.timestamp else None,
                'action': r.action,
                'session_id': r.session_id,
                'details': r.details or {}
            }
            for r in results
        ]
    finally:
        db.close()

def save_replicability_log(collection_hash, session_id, parameters, statistics, rate_limit_events=None, validation_results=None, notes=None):
    db = get_db_session()
    try:
        existing = db.query(ReplicabilityLog).filter_by(collection_hash=collection_hash).first()
        
        if existing:
            existing.parameters = parameters
            existing.statistics = statistics
            existing.rate_limit_events = rate_limit_events
            existing.validation_results = validation_results
            existing.notes = notes
            existing.session_id = session_id
        else:
            log = ReplicabilityLog(
                collection_hash=collection_hash,
                session_id=session_id,
                parameters=parameters,
                statistics=statistics,
                rate_limit_events=rate_limit_events,
                validation_results=validation_results,
                notes=notes
            )
            db.add(log)
        
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def load_replicability_logs(session_id=None, limit=50):
    db = get_db_session()
    try:
        query = db.query(ReplicabilityLog)
        
        if session_id:
            query = query.filter_by(session_id=session_id)
        
        query = query.order_by(ReplicabilityLog.timestamp.desc()).limit(limit)
        
        results = query.all()
        
        return [
            {
                'collection_hash': r.collection_hash,
                'timestamp': r.timestamp.isoformat() if r.timestamp else None,
                'session_id': r.session_id,
                'parameters': r.parameters or {},
                'statistics': r.statistics or {},
                'rate_limit_events': r.rate_limit_events or [],
                'validation_results': r.validation_results or {},
                'notes': r.notes
            }
            for r in results
        ]
    finally:
        db.close()

def get_data_quality_report():
    db = get_db_session()
    try:
        total_items = db.query(CollectedData).count()
        
        nsfw_items = db.query(CollectedData).filter(
            CollectedData.extra_metadata['nsfw'].astext == 'true'
        ).count() if total_items > 0 else 0
        
        removed_items = 0
        non_english_items = 0
        
        all_items = db.query(CollectedData).all()
        for item in all_items:
            if item.extra_metadata:
                if item.extra_metadata.get('content_status') in ['removed', 'author_deleted']:
                    removed_items += 1
                if item.extra_metadata.get('language_flag') == 'likely_non_english':
                    non_english_items += 1
        
        return {
            'total_items': total_items,
            'nsfw_items': nsfw_items,
            'removed_items': removed_items,
            'non_english_items': non_english_items,
            'report_generated': datetime.utcnow().isoformat()
        }
    finally:
        db.close()


def save_zotero_references(items, session_id):
    db = get_db_session()
    try:
        saved_count = 0
        for item in items:
            existing = db.query(ZoteroReference).filter_by(zotero_key=item.get('zotero_key')).first()
            
            if existing:
                existing.item_type = item.get('item_type')
                existing.title = item.get('title')
                existing.authors = item.get('authors', [])
                existing.year = item.get('year')
                existing.abstract = item.get('abstract')
                existing.doi = item.get('doi')
                existing.url = item.get('url')
                existing.tags = item.get('tags', [])
                existing.collections = item.get('collections', [])
                existing.keywords = item.get('keywords', [])
                existing.citation_apa = item.get('citation_apa')
                existing.synced_at = datetime.utcnow()
                existing.session_id = session_id
            else:
                record = ZoteroReference(
                    zotero_key=item.get('zotero_key'),
                    item_type=item.get('item_type'),
                    title=item.get('title'),
                    authors=item.get('authors', []),
                    year=item.get('year'),
                    abstract=item.get('abstract'),
                    doi=item.get('doi'),
                    url=item.get('url'),
                    tags=item.get('tags', []),
                    collections=item.get('collections', []),
                    keywords=item.get('keywords', []),
                    citation_apa=item.get('citation_apa'),
                    session_id=session_id
                )
                db.add(record)
            
            saved_count += 1
        
        db.commit()
        return saved_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def load_zotero_references(session_id=None, limit=200):
    db = get_db_session()
    try:
        query = db.query(ZoteroReference)
        
        if session_id:
            query = query.filter_by(session_id=session_id)
        
        query = query.order_by(ZoteroReference.synced_at.desc()).limit(limit)
        
        results = query.all()
        
        return [
            {
                'zotero_key': r.zotero_key,
                'item_type': r.item_type,
                'title': r.title,
                'authors': r.authors or [],
                'year': r.year,
                'abstract': r.abstract,
                'doi': r.doi,
                'url': r.url,
                'tags': r.tags or [],
                'collections': r.collections or [],
                'keywords': r.keywords or [],
                'citation_apa': r.citation_apa,
                'synced_at': r.synced_at.isoformat() if r.synced_at else None
            }
            for r in results
        ]
    finally:
        db.close()


def get_all_zotero_keywords():
    db = get_db_session()
    try:
        results = db.query(ZoteroReference).all()
        
        all_keywords = set()
        for r in results:
            if r.keywords:
                all_keywords.update(r.keywords)
        
        return sorted(list(all_keywords))
    finally:
        db.close()


def save_citation_links(links, session_id):
    db = get_db_session()
    try:
        saved_count = 0
        for link in links:
            existing = db.query(ZoteroCollectionLink).filter_by(
                collection_hash=link.get('collection_hash'),
                zotero_key=link.get('zotero_key')
            ).first()
            
            if not existing:
                record = ZoteroCollectionLink(
                    collection_hash=link.get('collection_hash'),
                    zotero_key=link.get('zotero_key'),
                    link_type=link.get('link_type', 'manual'),
                    relevance_score=link.get('relevance_score'),
                    matched_keywords=link.get('matched_keywords'),
                    session_id=session_id,
                    notes=link.get('notes')
                )
                db.add(record)
                saved_count += 1
        
        db.commit()
        return saved_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def load_citation_links(collection_hash=None, limit=100):
    db = get_db_session()
    try:
        query = db.query(ZoteroCollectionLink)
        
        if collection_hash:
            query = query.filter_by(collection_hash=collection_hash)
        
        query = query.order_by(ZoteroCollectionLink.linked_at.desc()).limit(limit)
        
        results = query.all()
        
        links_with_refs = []
        for r in results:
            ref = db.query(ZoteroReference).filter_by(zotero_key=r.zotero_key).first()
            
            links_with_refs.append({
                'collection_hash': r.collection_hash,
                'zotero_key': r.zotero_key,
                'link_type': r.link_type,
                'relevance_score': r.relevance_score,
                'matched_keywords': r.matched_keywords or [],
                'linked_at': r.linked_at.isoformat() if r.linked_at else None,
                'notes': r.notes,
                'citation': ref.citation_apa if ref else None,
                'title': ref.title if ref else None,
                'year': ref.year if ref else None
            })
        
        return links_with_refs
    finally:
        db.close()
