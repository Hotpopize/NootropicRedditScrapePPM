from database import get_db_session, CollectedData, CodedData, Codebook, AuditLog
from datetime import datetime
import json

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
                'collected_at': r.collected_at.isoformat() if r.collected_at else None,
                'metadata': r.extra_metadata or {}
            }
            for r in results
        ]
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

def save_codebook(codebook_dict, session_id):
    db = get_db_session()
    try:
        current_codes = set()
        for category, codes in codebook_dict.items():
            for code in codes:
                current_codes.add((category, code.get('name')))
                
                existing = db.query(Codebook).filter_by(
                    category=category,
                    name=code.get('name')
                ).first()
                
                if existing:
                    existing.definition = code.get('definition')
                    existing.examples = code.get('examples')
                    existing.frequency = code.get('frequency', 0)
                    existing.extra_metadata = code.get('metadata', {})
                    existing.session_id = session_id
                else:
                    record = Codebook(
                        category=category,
                        name=code.get('name'),
                        definition=code.get('definition'),
                        examples=code.get('examples'),
                        frequency=code.get('frequency', 0),
                        session_id=session_id,
                        extra_metadata=code.get('metadata', {})
                    )
                    db.add(record)
        
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
        
        codebook = {
            'push_factors': [],
            'pull_factors': [],
            'mooring_factors': [],
            'emergent_themes': []
        }
        
        seen_codes = {}
        for r in results:
            key = (r.category, r.name)
            if key in seen_codes:
                continue
            
            seen_codes[key] = True
            
            code_data = {
                'name': r.name,
                'definition': r.definition,
                'examples': r.examples,
                'frequency': r.frequency,
                'added_at': r.added_at.isoformat() if r.added_at else None,
                'metadata': r.extra_metadata or {}
            }
            
            if r.category in codebook:
                codebook[r.category].append(code_data)
        
        return codebook
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
