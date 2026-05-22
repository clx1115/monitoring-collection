from flask import Flask, render_template, request, jsonify
import pymysql
import os
import requests
import time
import threading
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')

# 批量采集状态管理
batch_collection_state = {
    'is_running': False,
    'is_cancelled': False,
    'current_index': 0,
    'total_count': 0,
    'current_builder': None,
    'completed_builders': [],
    'failed_builders': [],
    'start_time': None,
    'status_message': '未开始',
    'recovered': False  # 标记是否是恢复的任务
}

# 应用启动时尝试恢复批量采集状态
def recover_batch_collection_on_startup():
    """应用启动时检查是否有正在运行的批量采集任务"""
    global batch_collection_state
    
    try:
        print("\n" + "="*60)
        print("检查是否有正在运行的批量采集任务...")
        print("="*60)
        
        builders = get_collectable_builders()
        if not builders:
            print("没有可采集的建商")
            return
        
        # 检查每个建商的状态
        running_builders = []
        idle_builders = []
        
        for builder in builders:
            status_data = check_builder_status(builder['api_url'])
            if status_data.get('status') == 'running':
                running_builders.append(builder)
                print(f"✓ 发现 [{builder['builder_name']}] 正在采集中")
            elif status_data.get('status') in ['idle', 'completed']:
                idle_builders.append(builder)
        
        # 如果有建商正在运行，说明可能是批量采集任务
        if running_builders:
            print(f"\n发现 {len(running_builders)} 个建商正在采集")
            print("尝试恢复批量采集任务...")
            
            # 恢复状态
            batch_collection_state['is_running'] = True
            batch_collection_state['recovered'] = True
            batch_collection_state['total_count'] = len(builders)
            batch_collection_state['status_message'] = '恢复中...'
            batch_collection_state['start_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 启动恢复线程
            thread = threading.Thread(
                target=recovered_batch_collection_worker,
                args=(builders, running_builders)
            )
            thread.daemon = True
            thread.start()
            
            print("✓ 批量采集任务已恢复")
        else:
            print("没有发现正在运行的采集任务")
        
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"恢复批量采集任务失败: {str(e)}")

def recovered_batch_collection_worker(all_builders, running_builders):
    """恢复的批量采集工作线程"""
    global batch_collection_state
    
    try:
        print(f"\n{'='*50}")
        print(f"恢复批量采集任务")
        print(f"总建商数: {len(all_builders)}")
        print(f"正在运行: {len(running_builders)}")
        print(f"{'='*50}\n")
        
        # 先等待正在运行的建商完成
        for builder in running_builders:
            if batch_collection_state['is_cancelled']:
                break
            
            batch_collection_state['current_builder'] = {
                'id': builder['builder_id'],
                'name': builder['builder_name'],
                'api_url': builder['api_url']
            }
            batch_collection_state['status_message'] = f"等待完成: {builder['builder_name']}"
            
            print(f"[{builder['builder_name']}] 正在运行中，等待完成...")
            
            try:
                wait_for_builder_completion(
                    builder['api_url'],
                    builder['builder_name'],
                    check_interval=10
                )
                
                batch_collection_state['completed_builders'].append({
                    'id': builder['builder_id'],
                    'name': builder['builder_name'],
                    'status': 'success'
                })
                print(f"[{builder['builder_name']}] ✅ 采集完成")
                
            except Exception as e:
                batch_collection_state['failed_builders'].append({
                    'id': builder['builder_id'],
                    'name': builder['builder_name'],
                    'error': str(e)
                })
                print(f"[{builder['builder_name']}] ❌ 采集失败: {str(e)}")
        
        # 找出还未采集的建商
        completed_ids = {b['id'] for b in batch_collection_state['completed_builders']}
        failed_ids = {b['id'] for b in batch_collection_state['failed_builders']}
        running_ids = {b['builder_id'] for b in running_builders}
        
        pending_builders = [
            b for b in all_builders 
            if b['builder_id'] not in completed_ids 
            and b['builder_id'] not in failed_ids
            and b['builder_id'] not in running_ids
        ]
        
        # 继续采集剩余的建商
        if pending_builders and not batch_collection_state['is_cancelled']:
            print(f"\n继续采集剩余的 {len(pending_builders)} 个建商...\n")
            
            for i, builder in enumerate(pending_builders):
                if batch_collection_state['is_cancelled']:
                    break
                
                batch_collection_state['current_index'] = len(batch_collection_state['completed_builders']) + len(batch_collection_state['failed_builders']) + i
                batch_collection_state['current_builder'] = {
                    'id': builder['builder_id'],
                    'name': builder['builder_name'],
                    'api_url': builder['api_url']
                }
                batch_collection_state['status_message'] = f"正在采集: {builder['builder_name']}"
                
                print(f"[{builder['builder_name']}] 开始采集...")
                
                try:
                    start_result = start_builder_collection(builder['api_url'])
                    if start_result.get('status') not in ['success', 'warning']:
                        raise Exception(start_result.get('message', '启动失败'))
                    
                    wait_for_builder_completion(
                        builder['api_url'],
                        builder['builder_name'],
                        check_interval=10
                    )
                    
                    batch_collection_state['completed_builders'].append({
                        'id': builder['builder_id'],
                        'name': builder['builder_name'],
                        'status': 'success'
                    })
                    print(f"[{builder['builder_name']}] ✅ 采集完成")
                    
                except Exception as e:
                    batch_collection_state['failed_builders'].append({
                        'id': builder['builder_id'],
                        'name': builder['builder_name'],
                        'error': str(e)
                    })
                    print(f"[{builder['builder_name']}] ❌ 采集失败: {str(e)}")
                
                if i < len(pending_builders) - 1:
                    time.sleep(1)
        
        # 完成
        if not batch_collection_state['is_cancelled']:
            batch_collection_state['status_message'] = '全部完成'
            print(f"\n{'='*50}")
            print("批量采集全部完成！")
            print(f"成功: {len(batch_collection_state['completed_builders'])} 个")
            print(f"失败: {len(batch_collection_state['failed_builders'])} 个")
            print(f"{'='*50}\n")
        
    except Exception as e:
        print(f"恢复的批量采集发生错误: {str(e)}")
        batch_collection_state['status_message'] = f'错误: {str(e)}'
    
    finally:
        batch_collection_state['is_running'] = False

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'topskyhome_gather_log'),
    'charset': 'utf8mb4'
}

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取所有builder
    cursor.execute("SELECT * FROM builder ORDER BY builder_name")
    builders = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('index.html', builders=builders)

@app.route('/collection')
def collection():
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取所有builder，包括api_url和update_time字段，按sort字段排序
    cursor.execute("""
        SELECT builder_id, builder_name, api_url,
               DATE_FORMAT(update_time, '%%Y-%%m-%%d %%H:%%i:%%s') as update_time
        FROM builder 
        ORDER BY sort ASC, builder_name ASC
    """)
    builders = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('collection.html', builders=builders)

@app.route('/api/communities')
def get_communities():
    builder_id = request.args.get('builder_id', '').strip()
    update_status = request.args.get('update_status', '').strip()
    search = request.args.get('search', '').strip()
    
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 构建查询条件
    conditions = []
    params = []
    
    if builder_id:
        conditions.append("builder = %s")
        params.append(builder_id)
    
    if update_status != '':
        conditions.append("update_status = %s")
        params.append(int(update_status))
    
    if search:
        conditions.append("(name LIKE %s OR address LIKE %s)")
        params.extend([f'%{search}%', f'%{search}%'])
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
        SELECT id, name, address, url, status, update_status, uuid, archive_no,
               DATE_FORMAT(created_time, '%%Y-%%m-%%d %%H:%%i') as created_time,
               DATE_FORMAT(update_time, '%%Y-%%m-%%d %%H:%%i') as update_time
        FROM community_cache 
        WHERE {where_clause}
        ORDER BY update_time DESC
    """
    
    cursor.execute(query, params)
    communities = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify(communities)

@app.route('/detail/<int:community_id>')
def detail(community_id):
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取社区信息
    cursor.execute("""
        SELECT id, builder, name, address, url, json_url, status,update_status,
               DATE_FORMAT(created_time, '%%Y-%%m-%%d %%H:%%i:%%s') as created_time,
               DATE_FORMAT(update_time, '%%Y-%%m-%%d %%H:%%i:%%s') as update_time
        FROM community_cache 
        WHERE id = %s
    """, (community_id,))
    community = cursor.fetchone()
    
    # 获取更新日志
    cursor.execute("""
        SELECT id, builder, community_id, 
               floorplan_count, property_count, 
               property_sale, property_coming, property_sold,
               note,
               DATE_FORMAT(created_time, '%%Y-%%m-%%d %%H:%%i:%%s') as created_time
        FROM update_log 
        WHERE community_id = %s 
        ORDER BY created_time DESC
    """, (community_id,))
    logs = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('detail.html', community=community, logs=logs)

@app.route('/api/properties/<int:community_id>')
def get_properties(community_id):
    """获取社区的房源数据"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取房源数据
    cursor.execute("""
        SELECT id, property_uuid, community_id, 
               CAST(title AS CHAR) as title, 
               bedrooms, bathrooms, 
               size, price, lowest_price, 
               DATE_FORMAT(lowest_price_time, '%%Y-%%m-%%d %%H:%%i:%%s') as lowest_price_time,
               highest_price,
               DATE_FORMAT(highest_price_time, '%%Y-%%m-%%d %%H:%%i:%%s') as highest_price_time,
               status,
               DATE_FORMAT(created_time, '%%Y-%%m-%%d %%H:%%i:%%s') as created_time,
               DATE_FORMAT(update_time, '%%Y-%%m-%%d %%H:%%i:%%s') as update_time
        FROM property 
        WHERE community_id = %s 
        ORDER BY id ASC
    """, (community_id,))
    properties = cursor.fetchall()
    
    # 确保所有数据都能被JSON序列化
    serializable_properties = []
    for prop in properties:
        serializable_prop = {}
        for key, value in prop.items():
            if isinstance(value, bytes):
                serializable_prop[key] = value.decode('utf-8', errors='ignore')
            else:
                serializable_prop[key] = value
        serializable_properties.append(serializable_prop)
    
    cursor.close()
    conn.close()
    
    return jsonify(serializable_properties)

@app.route('/api/archive-detail/<uuid>/<int:archive_no>')
def get_archive_detail(uuid, archive_no):
    """获取指定采集次数的户型和房源详情"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取户型信息
    cursor.execute("""
        SELECT id, name, collection_name, home_type, 
               bedroom_num, bathroom_num, size, price,
               stories, lot_size
        FROM floorplan_archive 
        WHERE community_uuid = %s AND archive_no = %s
        ORDER BY name
    """, (uuid, archive_no))
    floorplans = cursor.fetchall()
    
    # 获取房源信息
    cursor.execute("""
        SELECT id, title, status, bedrooms, bathrooms, 
               size, listing_price, address
        FROM property_archive 
        WHERE community_uuid = %s AND archive_no = %s
        ORDER BY status, title
    """, (uuid, archive_no))
    properties = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'floorplans': floorplans,
        'properties': properties
    })

@app.route('/debug/archive/<uuid>')
def debug_archive(uuid):
    """调试路由：检查 community_archive 表中的数据"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 检查 community_cache 中的 uuid
    cursor.execute("SELECT id, name, uuid, archive_no FROM community_cache WHERE uuid = %s", (uuid,))
    cache_data = cursor.fetchone()
    
    # 检查 community_archive 中有多少条记录
    cursor.execute("SELECT COUNT(*) as count FROM community_archive WHERE uuid = %s", (uuid,))
    archive_count = cursor.fetchone()
    
    # 获取前5条记录看看
    cursor.execute("""
        SELECT id, name, uuid, archive_no, archive_time, created_at 
        FROM community_archive 
        WHERE uuid = %s 
        LIMIT 5
    """, (uuid,))
    sample_archives = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'uuid': uuid,
        'cache_data': cache_data,
        'archive_count': archive_count,
        'sample_archives': sample_archives
    })

@app.route('/history/<uuid>')
def history(uuid):
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取社区基本信息（从community_cache）
    cursor.execute("""
        SELECT id, name, address, builder, uuid, archive_no
        FROM community_cache 
        WHERE uuid = %s
    """, (uuid,))
    community = cursor.fetchone()
    
    if not community:
        cursor.close()
        conn.close()
        return "社区不存在", 404
    
    # 获取历史采集记录（从community_archive）
    # 使用 COALESCE 处理 NULL 值，并按 archive_no 或 id 排序
    cursor.execute("""
        SELECT id, name, address, builder, 
               COALESCE(archive_no, 0) as archive_no,
               price, price_max,
               size_min, size_max,
               bedrooms, bedrooms_max, bathrooms, bathrooms_max,
               DATE_FORMAT(COALESCE(archive_time, created_at, updated_at), '%%Y-%%m-%%d %%H:%%i:%%s') as archive_time,
               DATE_FORMAT(created_at, '%%Y-%%m-%%d %%H:%%i:%%s') as created_at
        FROM community_archive 
        WHERE uuid = %s 
        ORDER BY COALESCE(archive_no, id) DESC
    """, (uuid,))
    archives = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('history.html', community=community, archives=archives)

# ==================== 批量采集功能 ====================

def reset_batch_state():
    """重置批量采集状态"""
    global batch_collection_state
    batch_collection_state = {
        'is_running': False,
        'is_cancelled': False,
        'current_index': 0,
        'total_count': 0,
        'current_builder': None,
        'completed_builders': [],
        'failed_builders': [],
        'start_time': None,
        'status_message': '未开始'
    }

def get_collectable_builders():
    """获取所有配置了 API 的建商，按 sort 字段排序"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    cursor.execute("""
        SELECT builder_id, builder_name, api_url 
        FROM builder 
        WHERE api_url IS NOT NULL AND api_url != ''
        ORDER BY sort ASC, builder_name ASC
    """)
    builders = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return builders

def check_builder_status(api_url):
    """检查单个建商的采集状态"""
    try:
        response = requests.get(f"{api_url}/api/status", timeout=5)
        if response.status_code == 200:
            return response.json()
        return {'status': 'error', 'message': f'HTTP {response.status_code}'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def start_builder_collection(api_url):
    """启动单个建商的采集"""
    try:
        response = requests.post(f"{api_url}/api/start", timeout=10)
        if response.status_code == 200:
            return response.json()
        return {'status': 'error', 'message': f'HTTP {response.status_code}'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def wait_for_builder_completion(api_url, builder_name, check_interval=10):
    """等待建商采集完成（无超时限制，一直等到完成）
    
    Args:
        api_url: 建商API地址
        builder_name: 建商名称
        check_interval: 检查间隔（秒），默认10秒
    """
    global batch_collection_state
    start_time = time.time()
    last_status = None
    error_count = 0
    max_errors = 10  # 允许的最大连续错误次数
    
    print(f"[{builder_name}] 开始等待采集完成（无超时限制，直到完成）...")
    
    while True:
        # 检查是否被取消
        if batch_collection_state['is_cancelled']:
            print(f"[{builder_name}] 采集被取消")
            raise Exception('已取消')
        
        # 检查状态
        try:
            status_data = check_builder_status(api_url)
            current_status = status_data.get('status')
            
            # 重置错误计数
            if current_status != 'error':
                error_count = 0
            
            # 只在状态变化时打印，或者每5分钟打印一次进度
            elapsed = int(time.time() - start_time)
            should_print = (current_status != last_status) or (elapsed > 0 and elapsed % 300 == 0)
            
            if should_print:
                minutes = elapsed // 60
                seconds = elapsed % 60
                time_str = f"{minutes}分{seconds}秒" if minutes > 0 else f"{seconds}秒"
                print(f"[{builder_name}] 状态: {current_status} (已等待 {time_str})")
                last_status = current_status
            
            # 检查是否完成
            if current_status in ['idle', 'completed']:
                minutes = elapsed // 60
                seconds = elapsed % 60
                time_str = f"{minutes}分{seconds}秒" if minutes > 0 else f"{seconds}秒"
                print(f"[{builder_name}] ✅ 采集完成 (耗时 {time_str})")
                return
            
            # 如果状态是 error，增加错误计数
            if current_status == 'error':
                error_count += 1
                if error_count >= max_errors:
                    print(f"[{builder_name}] ❌ 连续 {max_errors} 次获取状态失败")
                    raise Exception(f'连续 {max_errors} 次状态检查失败')
            
        except Exception as e:
            # 如果是我们主动抛出的异常，直接向上传递
            if '已取消' in str(e) or '状态检查失败' in str(e):
                raise
            # 其他异常只记录，不中断
            error_count += 1
            print(f"[{builder_name}] ⚠️  状态检查异常 ({error_count}/{max_errors}): {str(e)}")
            if error_count >= max_errors:
                raise Exception(f'连续 {max_errors} 次状态检查异常')
        
        # 等待后再次检查
        time.sleep(check_interval)

def batch_collection_worker():
    """批量采集工作线程"""
    global batch_collection_state
    
    try:
        builders = get_collectable_builders()
        batch_collection_state['total_count'] = len(builders)
        batch_collection_state['start_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n{'='*50}")
        print(f"开始批量采集，共 {len(builders)} 个建商")
        print(f"{'='*50}\n")
        
        for i, builder in enumerate(builders):
            # 检查是否被取消
            if batch_collection_state['is_cancelled']:
                print("批量采集已取消")
                batch_collection_state['status_message'] = '已取消'
                break
            
            batch_collection_state['current_index'] = i
            batch_collection_state['current_builder'] = {
                'id': builder['builder_id'],
                'name': builder['builder_name'],
                'api_url': builder['api_url']
            }
            batch_collection_state['status_message'] = f"正在采集: {builder['builder_name']}"
            
            print(f"\n{'='*50}")
            print(f"[{i+1}/{len(builders)}] 开始采集: {builder['builder_name']}")
            print(f"建商ID: {builder['builder_id']}")
            print(f"API地址: {builder['api_url']}")
            print(f"{'='*50}\n")
            
            try:
                # 启动采集
                print(f"[{builder['builder_name']}] 📤 发送启动请求...")
                start_result = start_builder_collection(builder['api_url'])
                
                if start_result.get('status') not in ['success', 'warning']:
                    raise Exception(f"启动失败: {start_result.get('message', '未知错误')}")
                
                # 如果是 warning，说明已经在运行中
                if start_result.get('status') == 'warning':
                    print(f"[{builder['builder_name']}] ⚠️  {start_result.get('message', '已在运行中')}")
                else:
                    print(f"[{builder['builder_name']}] ✅ 启动成功")
                
                print(f"[{builder['builder_name']}] ⏳ 等待采集完成...")
                
                # 等待完成（无超时限制）
                wait_for_builder_completion(
                    builder['api_url'], 
                    builder['builder_name'],
                    check_interval=10
                )
                
                # 记录成功
                batch_collection_state['completed_builders'].append({
                    'id': builder['builder_id'],
                    'name': builder['builder_name'],
                    'status': 'success'
                })
                print(f"[{builder['builder_name']}] 🎉 采集成功完成\n")
                
            except Exception as e:
                # 记录失败
                error_msg = str(e)
                batch_collection_state['failed_builders'].append({
                    'id': builder['builder_id'],
                    'name': builder['builder_name'],
                    'error': error_msg
                })
                print(f"[{builder['builder_name']}] ❌ 采集失败: {error_msg}\n")
                
                # 如果是取消操作，立即退出循环
                if '已取消' in error_msg:
                    break
            
            # 短暂延迟
            if i < len(builders) - 1 and not batch_collection_state['is_cancelled']:
                print(f"等待1秒后继续下一个建商...")
                time.sleep(1)
        
        # 完成
        if not batch_collection_state['is_cancelled']:
            batch_collection_state['status_message'] = '全部完成'
            print(f"\n{'='*50}")
            print("批量采集全部完成！")
            print(f"成功: {len(batch_collection_state['completed_builders'])} 个")
            print(f"失败: {len(batch_collection_state['failed_builders'])} 个")
            print(f"{'='*50}\n")
        
    except Exception as e:
        print(f"批量采集发生错误: {str(e)}")
        batch_collection_state['status_message'] = f'错误: {str(e)}'
    
    finally:
        batch_collection_state['is_running'] = False

@app.route('/api/batch-collection/start', methods=['POST'])
def start_batch_collection():
    """启动批量采集"""
    global batch_collection_state
    
    # 检查是否已有任务在运行
    if batch_collection_state['is_running']:
        return jsonify({
            'status': 'error',
            'message': '已有批量采集任务正在运行中'
        }), 400
    
    # 检查是否有单个建商正在采集
    builders = get_collectable_builders()
    for builder in builders:
        status_data = check_builder_status(builder['api_url'])
        if status_data.get('status') == 'running':
            return jsonify({
                'status': 'error',
                'message': f"{builder['builder_name']} 正在采集中，请等待完成"
            }), 400
    
    if not builders:
        return jsonify({
            'status': 'error',
            'message': '没有可采集的建商（需要配置API）'
        }), 400
    
    # 重置状态并启动
    reset_batch_state()
    batch_collection_state['is_running'] = True
    batch_collection_state['status_message'] = '准备开始...'
    
    # 在后台线程中执行
    thread = threading.Thread(target=batch_collection_worker)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': f'批量采集已启动，共 {len(builders)} 个建商',
        'total_count': len(builders)
    })

@app.route('/api/batch-collection/cancel', methods=['POST'])
def cancel_batch_collection():
    """取消批量采集"""
    global batch_collection_state
    
    if not batch_collection_state['is_running']:
        return jsonify({
            'status': 'error',
            'message': '没有正在运行的批量采集任务'
        }), 400
    
    batch_collection_state['is_cancelled'] = True
    batch_collection_state['status_message'] = '正在取消...'
    
    return jsonify({
        'status': 'success',
        'message': '批量采集取消请求已发送'
    })

@app.route('/api/batch-collection/status', methods=['GET'])
def get_batch_collection_status():
    """获取批量采集状态"""
    global batch_collection_state
    
    return jsonify({
        'is_running': batch_collection_state['is_running'],
        'is_cancelled': batch_collection_state['is_cancelled'],
        'current_index': batch_collection_state['current_index'],
        'total_count': batch_collection_state['total_count'],
        'current_builder': batch_collection_state['current_builder'],
        'completed_count': len(batch_collection_state['completed_builders']),
        'failed_count': len(batch_collection_state['failed_builders']),
        'completed_builders': batch_collection_state['completed_builders'],
        'failed_builders': batch_collection_state['failed_builders'],
        'start_time': batch_collection_state['start_time'],
        'status_message': batch_collection_state['status_message'],
        'recovered': batch_collection_state.get('recovered', False)  # 是否是恢复的任务
    })

# ==================== 批量采集功能结束 ====================

if __name__ == '__main__':
    # 应用启动时尝试恢复批量采集任务（已禁用，避免重复启动采集）
    # recover_batch_collection_on_startup()
    
    debug = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', 5000))
    app.run(debug=debug, host=host, port=port)
