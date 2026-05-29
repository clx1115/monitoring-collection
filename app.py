from flask import Flask, render_template, request, jsonify
import pymysql
import os
import requests
import time
import threading
from datetime import datetime
from dotenv import load_dotenv
from dbutils.pooled_db import PooledDB


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

# 创建数据库连接池，提高并发效能，并具有自动心跳检测和重连机制
db_pool = PooledDB(
    creator=pymysql,
    maxconnections=10,  # 最大连接数
    mincached=2,        # 最小空闲连接数
    maxcached=5,        # 最大空闲连接数
    blocking=True,      # 连接满时阻塞等待
    ping=1,             # 获取连接前进行 ping 检查 (如失效自动重连)
    **DB_CONFIG
)

def get_db_connection(max_retries=3, delay=1):
    """获取数据库连接，内置重试与指数退避机制以应对不稳定的远程网络连接"""
    for i in range(max_retries):
        try:
            conn = db_pool.connection()
            # 显式执行心跳校验并强制重连
            conn.ping(reconnect=True)
            return conn
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            print(f"⚠️ [Database Connection Error] (Retry {i+1}/{max_retries}): {str(e)}")
            if i == max_retries - 1:
                raise
            time.sleep(delay * (2 ** i))


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

# ==================== 房源走势图功能 ====================

@app.route('/property-trend/<int:community_id>/<property_uuid>')
def property_trend(community_id, property_uuid):
    """显示某个房源的价格和状态走势图页面"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取社区信息
    cursor.execute("""
        SELECT id, name, uuid FROM community_cache WHERE id = %s
    """, (community_id,))
    community = cursor.fetchone()
    
    if not community:
        cursor.close()
        conn.close()
        return "社区不存在", 404
    
    # 获取房源当前信息
    cursor.execute("""
        SELECT id, property_uuid, community_id,
               CAST(title AS CHAR) as title,
               bedrooms, bathrooms, size, price, status,
               lowest_price, highest_price,
               DATE_FORMAT(lowest_price_time, '%%Y-%%m-%%d %%H:%%i:%%s') as lowest_price_time,
               DATE_FORMAT(highest_price_time, '%%Y-%%m-%%d %%H:%%i:%%s') as highest_price_time,
               DATE_FORMAT(created_time, '%%Y-%%m-%%d %%H:%%i:%%s') as created_time,
               DATE_FORMAT(update_time, '%%Y-%%m-%%d %%H:%%i:%%s') as update_time
        FROM property
        WHERE community_id = %s AND property_uuid = %s
    """, (community_id, property_uuid))
    property_info = cursor.fetchone()
    
    if not property_info:
        cursor.close()
        conn.close()
        return "房源不存在", 404
    
    cursor.close()
    conn.close()
    
    return render_template('property_trend.html', 
                           community=community, 
                           property=property_info)


@app.route('/api/property-history/<int:community_id>/<property_uuid>')
def get_property_history(community_id, property_uuid):
    """获取房源历史价格和状态数据"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取社区UUID和房源title
    cursor.execute("SELECT uuid FROM community_cache WHERE id = %s", (community_id,))
    community = cursor.fetchone()
    if not community:
        cursor.close()
        conn.close()
        return jsonify({'error': '社区不存在'}), 404
    
    cursor.execute("""
        SELECT CAST(title AS CHAR) as title
        FROM property
        WHERE community_id = %s AND property_uuid = %s
    """, (community_id, property_uuid))
    prop = cursor.fetchone()
    if not prop:
        cursor.close()
        conn.close()
        return jsonify({'error': '房源不存在'}), 404
    
    community_uuid = community['uuid']
    property_title = prop['title']
    
    # 获取该房源在各次采集中的历史数据
    cursor.execute("""
        SELECT pa.listing_price, pa.status, pa.archive_no,
               CAST(pa.title AS CHAR) as title,
               ca.archive_time
        FROM property_archive pa
        LEFT JOIN community_archive ca 
            ON ca.uuid = pa.community_uuid AND ca.archive_no = pa.archive_no
        WHERE pa.community_uuid = %s AND pa.uuid = %s
        ORDER BY pa.archive_no ASC
    """, (community_uuid, property_uuid))
    history = cursor.fetchall()
    
    # 获取社区采集时间轴
    cursor.execute("""
        SELECT archive_no, 
               DATE_FORMAT(archive_time, '%%Y-%%m-%%d %%H:%%i:%%s') as archive_time
        FROM community_archive
        WHERE uuid = %s
        ORDER BY archive_no ASC
    """, (community_uuid,))
    archive_times = cursor.fetchall()
    archive_time_map = {a['archive_no']: a['archive_time'] for a in archive_times}
    
    cursor.close()
    conn.close()
    
    # 格式化数据
    result = []
    for item in history:
        result.append({
            'archive_no': item['archive_no'],
            'archive_time': archive_time_map.get(item['archive_no'], f'采集#{item["archive_no"]}'),
            'listing_price': item['listing_price'],
            'status': item['status']
        })
    
    return jsonify(result)


@app.route('/api/property-changes/<int:community_id>')
def get_property_changes(community_id):
    """获取社区中变化最大的房源TOP20"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取社区UUID
    cursor.execute("SELECT uuid FROM community_cache WHERE id = %s", (community_id,))
    community = cursor.fetchone()
    if not community:
        cursor.close()
        conn.close()
        return jsonify({'error': '社区不存在'}), 404
    
    community_uuid = community['uuid']
    
    # 获取当前房源
    cursor.execute("""
        SELECT id, property_uuid, community_id,
               CAST(title AS CHAR) as title,
               bedrooms, bathrooms, size, price, status,
               lowest_price, highest_price,
               DATE_FORMAT(created_time, '%%Y-%%m-%%d %%H:%%i:%%s') as created_time,
               DATE_FORMAT(update_time, '%%Y-%%m-%%d %%H:%%i:%%s') as update_time
        FROM property
        WHERE community_id = %s
    """, (community_id,))
    properties = cursor.fetchall()
    
    # 获取归档数据
    cursor.execute("""
        SELECT CAST(title AS CHAR) as title, listing_price, status, archive_no
        FROM property_archive
        WHERE community_uuid = %s
        ORDER BY archive_no ASC
    """, (community_uuid,))
    archives = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # 按title分组归档数据
    from collections import defaultdict
    title_archives = defaultdict(list)
    for a in archives:
        title_archives[a['title']].append(a)
    
    # 计算每个房源的变化指标
    property_changes = []
    for prop in properties:
        title = prop['title']
        archive_list = title_archives.get(title, [])
        
        # 统计状态变化次数
        status_changes = 0
        if len(archive_list) > 1:
            for i in range(1, len(archive_list)):
                if archive_list[i]['status'] != archive_list[i-1]['status']:
                    status_changes += 1
        
        # 统计价格变化次数
        price_changes = 0
        if len(archive_list) > 1:
            for i in range(1, len(archive_list)):
                pa = archive_list[i]['listing_price']
                pb = archive_list[i-1]['listing_price']
                if pa is not None and pb is not None and pa != pb:
                    price_changes += 1
        
        # 计算价格波动范围
        max_price = prop['highest_price'] or prop['price'] or 0
        min_price = prop['lowest_price'] or prop['price'] or 0
        price_range = max_price - min_price
        price_range_pct = round((price_range / max_price * 100), 1) if max_price > 0 else 0
        
        # 综合变化评分 (状态变化权重2 + 价格变化 + 价格范围贡献)
        change_score = status_changes * 2 + price_changes
        if price_range > 0:
            change_score += round(price_range / 10000.0, 2)
        
        property_changes.append({
            'title': title,
            'property_uuid': prop['property_uuid'],
            'community_id': prop['community_id'],
            'bedrooms': prop['bedrooms'],
            'bathrooms': prop['bathrooms'],
            'size': prop['size'],
            'price': prop['price'],
            'status': prop['status'],
            'lowest_price': prop['lowest_price'],
            'highest_price': prop['highest_price'],
            'status_changes': status_changes,
            'price_changes': price_changes,
            'price_range': price_range,
            'price_range_pct': price_range_pct,
            'archive_count': len(archive_list),
            'change_score': round(change_score, 2)
        })
    
    # 按综合评分降序排列，取TOP20
    property_changes.sort(key=lambda x: x['change_score'], reverse=True)
    
    return jsonify(property_changes[:20])


# ==================== 统计模块 ====================

@app.route('/stats')
def stats():
    """统计页面"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取所有builder用于筛选下拉框
    cursor.execute("SELECT builder_id, builder_name FROM builder ORDER BY builder_name")
    builders = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('stats.html', builders=builders)


@app.route('/property-stats/<int:community_id>')
def property_stats(community_id):
    """房源统计页面"""
    community_name = request.args.get('name', '未知社区')
    community_uuid = request.args.get('uuid', '')
    
    return render_template('property_stats.html', 
                         community_id=community_id,
                         community_name=community_name,
                         community_uuid=community_uuid)


@app.route('/api/stats/communities')
def get_stats_communities():
    """获取社区列表（带分页和过滤）"""
    # 获取查询参数
    builder_id = request.args.get('builder_id', '').strip()
    keyword = request.args.get('keyword', '').strip()
    prop_filter = request.args.get('prop_filter', 'active').strip()
    sort_by = request.args.get('sort_by', 'update_time').strip()
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 20, type=int)
    
    # 限制每页条数
    if page_size not in [10, 20, 50, 100]:
        page_size = 20
    if page < 1:
        page = 1
    
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 构建查询条件
    conditions = []
    params = []
    
    if builder_id:
        conditions.append("cc.builder = %s")
        params.append(int(builder_id))
    
    if keyword:
        conditions.append("(cc.name LIKE %s OR cc.address LIKE %s)")
        params.extend([f'%{keyword}%', f'%{keyword}%'])
        
    if prop_filter == 'active':
        conditions.append("EXISTS (SELECT 1 FROM property p WHERE p.community_id = cc.id)")
    elif prop_filter == 'empty':
        conditions.append("NOT EXISTS (SELECT 1 FROM property p WHERE p.community_id = cc.id)")
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    # 动态构建排序
    order_by_clause = "cc.update_time DESC"
    if sort_by == 'prop_count':
        order_by_clause = "(SELECT COUNT(*) FROM property p WHERE p.community_id = cc.id) DESC, cc.update_time DESC"
    elif sort_by == 'archive_no':
        order_by_clause = "cc.archive_no DESC, cc.update_time DESC"
    
    # 查询总数
    count_query = f"""
        SELECT COUNT(*) as total
        FROM community_cache cc
        WHERE {where_clause}
    """
    cursor.execute(count_query, params)
    total = cursor.fetchone()['total']
    
    # 计算分页
    offset = (page - 1) * page_size
    total_pages = (total + page_size - 1) // page_size
    
    # 查询数据（关联builder表获取建商名称）
    data_query = f"""
        SELECT cc.id, cc.name, cc.address, cc.url, cc.json_url, 
               cc.status, cc.uuid, cc.archive_no,
               (SELECT COUNT(*) FROM property p WHERE p.community_id = cc.id) as property_count,
               DATE_FORMAT(cc.created_time, '%%Y-%%m-%%d %%H:%%i') as created_time,
               DATE_FORMAT(cc.update_time, '%%Y-%%m-%%d %%H:%%i') as update_time,
               b.builder_name
        FROM community_cache cc
        LEFT JOIN builder b ON cc.builder = b.builder_id
        WHERE {where_clause}
        ORDER BY {order_by_clause}
        LIMIT %s OFFSET %s
    """
    cursor.execute(data_query, params + [page_size, offset])
    communities = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'data': communities,
        'pagination': {
            'page': page,
            'page_size': page_size,
            'total': total,
            'total_pages': total_pages
        }
    })


@app.route('/api/stats/archive/<uuid>')
def get_archive_stats(uuid):
    """获取社区每轮采集的户型和房源数量统计"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 查询每轮采集的户型数量
    cursor.execute("""
        SELECT archive_no, 
               COUNT(*) as floorplan_count,
               DATE_FORMAT(MIN(archive_time), '%%Y-%%m-%%d %%H:%%i') as archive_time
        FROM floorplan_archive 
        WHERE community_uuid = %s
        GROUP BY archive_no
        ORDER BY archive_no ASC
    """, (uuid,))
    floorplan_stats = cursor.fetchall()
    
    # 查询每轮采集的房源数量
    cursor.execute("""
        SELECT archive_no, 
               COUNT(*) as property_count
        FROM property_archive 
        WHERE community_uuid = %s
        GROUP BY archive_no
        ORDER BY archive_no ASC
    """, (uuid,))
    property_stats = cursor.fetchall()
    
    # 获取社区的采集时间
    cursor.execute("""
        SELECT archive_no,
               DATE_FORMAT(archive_time, '%%Y-%%m-%%d %%H:%%i') as archive_time
        FROM community_archive
        WHERE uuid = %s
        ORDER BY archive_no ASC
    """, (uuid,))
    community_archives = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # 合并数据
    archive_time_map = {item['archive_no']: item['archive_time'] for item in community_archives}
    floorplan_map = {item['archive_no']: item['floorplan_count'] for item in floorplan_stats}
    property_map = {item['archive_no']: item['property_count'] for item in property_stats}
    
    # 获取所有轮次
    all_archive_nos = sorted(set(
        list(floorplan_map.keys()) + 
        list(property_map.keys()) + 
        list(archive_time_map.keys())
    ))
    
    result = []
    for archive_no in all_archive_nos:
        result.append({
            'archive_no': archive_no,
            'archive_time': archive_time_map.get(archive_no, '-'),
            'floorplan_count': floorplan_map.get(archive_no, 0),
            'property_count': property_map.get(archive_no, 0)
        })
    
    return jsonify(result)


@app.route('/api/stats/price-trend/<uuid>')
def get_price_trend(uuid):
    """获取社区价格趋势数据"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 查询每轮采集的价格统计
    cursor.execute("""
        SELECT archive_no,
               COUNT(*) as property_count,
               MIN(listing_price) as min_price,
               MAX(listing_price) as max_price,
               AVG(listing_price) as avg_price,
               SUM(CASE WHEN status = 'For Sale' THEN 1 ELSE 0 END) as for_sale_count,
               SUM(CASE WHEN status = 'Coming Soon' THEN 1 ELSE 0 END) as coming_soon_count,
               SUM(CASE WHEN status = 'Sold' THEN 1 ELSE 0 END) as sold_count
        FROM property_archive 
        WHERE community_uuid = %s AND listing_price IS NOT NULL AND listing_price > 0
        GROUP BY archive_no
        ORDER BY archive_no ASC
    """, (uuid,))
    price_stats = cursor.fetchall()
    
    # 获取社区的采集时间
    cursor.execute("""
        SELECT archive_no,
               DATE_FORMAT(archive_time, '%%Y-%%m-%%d %%H:%%i') as archive_time
        FROM community_archive
        WHERE uuid = %s
        ORDER BY archive_no ASC
    """, (uuid,))
    community_archives = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # 合并数据
    archive_time_map = {item['archive_no']: item['archive_time'] for item in community_archives}
    
    result = []
    for item in price_stats:
        archive_no = item['archive_no']
        result.append({
            'archive_no': archive_no,
            'archive_time': archive_time_map.get(archive_no, '-'),
            'property_count': item['property_count'],
            'min_price': round(item['min_price']) if item['min_price'] else 0,
            'max_price': round(item['max_price']) if item['max_price'] else 0,
            'avg_price': round(item['avg_price']) if item['avg_price'] else 0,
            'for_sale_count': item['for_sale_count'] or 0,
            'coming_soon_count': item['coming_soon_count'] or 0,
            'sold_count': item['sold_count'] or 0
        })
    
    return jsonify(result)


# ==================== 批量采集功能结束 ====================



@app.route('/api/stats/properties')
def get_stats_properties():
    """获取所有房源列表（带分页和过滤）"""
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 24, type=int)
    
    builder_id = request.args.get('builder_id', '').strip()
    community_id = request.args.get('community_id', '').strip()
    keyword = request.args.get('keyword', '').strip()
    status = request.args.get('status', '').strip()
    bedrooms = request.args.get('bedrooms', '').strip()
    bathrooms = request.args.get('bathrooms', '').strip()
    
    min_price = request.args.get('min_price', type=float)
    max_price = request.args.get('max_price', type=float)
    min_size = request.args.get('min_size', type=float)
    max_size = request.args.get('max_size', type=float)
    
    has_price_drop = request.args.get('has_price_drop', '').strip().lower() in ['true', '1']
    at_historical_low = request.args.get('at_historical_low', '').strip().lower() in ['true', '1']
    sort_by = request.args.get('sort_by', 'price-asc').strip()

    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 24
        
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    conditions = []
    params = []
    
    if builder_id:
        conditions.append("cc.builder = %s")
        params.append(int(builder_id))
        
    if community_id:
        conditions.append("p.community_id = %s")
        params.append(int(community_id))
        
    if keyword:
        conditions.append("(CAST(p.title AS CHAR) LIKE %s OR p.address LIKE %s)")
        params.extend([f'%{keyword}%', f'%{keyword}%'])
        
    if status:
        conditions.append("p.status = %s")
        params.append(status)
        
    if bedrooms:
        conditions.append("p.bedrooms >= %s")
        params.append(int(bedrooms))
        
    if bathrooms:
        conditions.append("p.bathrooms >= %s")
        params.append(float(bathrooms))
        
    if min_price is not None:
        conditions.append("p.price >= %s")
        params.append(min_price)
        
    if max_price is not None:
        conditions.append("p.price > 0 AND p.price <= %s")
        params.append(max_price)
        
    if min_size is not None:
        conditions.append("p.size >= %s")
        params.append(min_size)
        
    if max_size is not None:
        conditions.append("p.size <= %s")
        params.append(max_size)
        
    if has_price_drop:
        conditions.append("p.price > 0 AND p.highest_price > p.price")
        
    if at_historical_low:
        conditions.append("p.price > 0 AND p.lowest_price > 0 AND p.price <= p.lowest_price")
        
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    # 动态构建排序规则
    order_by_clause = "p.price ASC"
    if sort_by == 'price-desc':
        order_by_clause = "p.price DESC"
    elif sort_by == 'bedrooms':
        order_by_clause = "p.bedrooms DESC, p.price ASC"
    elif sort_by == 'size':
        order_by_clause = "p.size DESC, p.price ASC"
    elif sort_by == 'drop-pct':
        order_by_clause = "CASE WHEN p.highest_price > p.price THEN (p.highest_price - p.price) / p.highest_price ELSE 0 END DESC, p.price ASC"
    elif sort_by == 'drop-amount':
        order_by_clause = "CASE WHEN p.highest_price > p.price THEN (p.highest_price - p.price) ELSE 0 END DESC, p.price ASC"

    # 查询总数
    count_query = f"""
        SELECT COUNT(*) as total
        FROM property p
        JOIN community_cache cc ON p.community_id = cc.id
        WHERE {where_clause}
    """
    cursor.execute(count_query, params)
    total = cursor.fetchone()['total']
    
    # 计算分页
    offset = (page - 1) * page_size
    total_pages = (total + page_size - 1) // page_size
    
    # 查询当前页的房源
    data_query = f"""
        SELECT p.id, p.property_uuid, p.community_id,
               CAST(p.title AS CHAR) as title,
               p.bedrooms, p.bathrooms, p.size, p.price,
               p.lowest_price, p.highest_price, p.status,
               DATE_FORMAT(p.created_time, '%%Y-%%m-%%d %%H:%%i:%%s') as created_time,
               DATE_FORMAT(p.update_time, '%%Y-%%m-%%d %%H:%%i:%%s') as update_time,
               cc.name as community_name, cc.uuid as community_uuid
        FROM property p
        JOIN community_cache cc ON p.community_id = cc.id
        WHERE {where_clause}
        ORDER BY {order_by_clause}
        LIMIT %s OFFSET %s
    """
    cursor.execute(data_query, params + [page_size, offset])
    properties = cursor.fetchall()
    
    # 确保JSON序列化
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
    
    return jsonify({
        'data': serializable_properties,
        'pagination': {
            'page': page,
            'page_size': page_size,
            'total': total,
            'total_pages': total_pages
        }
    })


# ==================== 数据智能洞察 API ====================

@app.route('/api/insights/price-drops')
def get_insight_price_drops():
    """获取近期降价幅度最大的房源 Top 10"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    community_id = request.args.get('community_id', '').strip()
    builder_id = request.args.get('builder_id', '').strip()
    
    query = """
        SELECT p.id, CAST(p.title AS CHAR) as title, p.property_uuid,
               p.community_id, p.bedrooms, p.bathrooms, p.size,
               p.price as current_price, p.highest_price,
               (p.highest_price - p.price) as drop_amount,
               ROUND((p.highest_price - p.price) / p.highest_price * 100, 1) as drop_pct,
               p.status,
               DATE_FORMAT(p.update_time, '%%Y-%%m-%%d %%H:%%i') as update_time,
               cc.name as community_name, cc.uuid as community_uuid,
               b.builder_name
        FROM property p
        JOIN community_cache cc ON p.community_id = cc.id
        LEFT JOIN builder b ON cc.builder = b.builder_id
        WHERE p.status != 'Sold'
          AND p.price > 0
          AND p.highest_price > p.price
    """
    params = []
    if community_id:
        query += " AND p.community_id = %s"
        params.append(int(community_id))
    if builder_id:
        query += " AND cc.builder = %s"
        params.append(int(builder_id))
        
    query += " ORDER BY drop_amount DESC LIMIT 10"
    
    cursor.execute(query, params)
    results = cursor.fetchall()

    serializable = []
    for row in results:
        s = {}
        for k, v in row.items():
            if isinstance(v, bytes):
                s[k] = v.decode('utf-8', errors='ignore')
            else:
                s[k] = v
        serializable.append(s)

    cursor.close()
    conn.close()
    return jsonify(serializable)


@app.route('/api/insights/active-value')
def get_insight_active_value():
    """获取最受关注/状态频繁变更的高性价比户型 Top 10"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    community_id = request.args.get('community_id', '').strip()
    builder_id = request.args.get('builder_id', '').strip()

    query = """
        SELECT p.id, CAST(p.title AS CHAR) as title, p.property_uuid,
               p.community_id, p.bedrooms, p.bathrooms, p.size,
               p.price, p.status,
               ROUND(p.price / p.size, 0) as price_per_sqft,
               p.lowest_price, p.highest_price,
               cc.name as community_name, cc.uuid as community_uuid,
               b.builder_name
        FROM property p
        JOIN community_cache cc ON p.community_id = cc.id
        LEFT JOIN builder b ON cc.builder = b.builder_id
        WHERE p.status != 'Sold'
          AND p.price > 0
          AND p.size > 0
    """
    params = []
    if community_id:
        query += " AND p.community_id = %s"
        params.append(int(community_id))
    if builder_id:
        query += " AND cc.builder = %s"
        params.append(int(builder_id))
        
    query += " ORDER BY price_per_sqft ASC LIMIT 50"
    
    cursor.execute(query, params)
    candidates = cursor.fetchall()

    # Step 2: For each candidate, count activity from property_archive
    results = []
    for prop in candidates:
        cursor.execute("""
            SELECT listing_price, status, archive_no
            FROM property_archive
            WHERE community_uuid = %s AND uuid = %s
            ORDER BY archive_no ASC
        """, (prop['community_uuid'], prop['property_uuid']))
        archives = cursor.fetchall()

        status_changes = 0
        price_changes = 0
        if len(archives) > 1:
            for i in range(1, len(archives)):
                if archives[i]['status'] != archives[i-1]['status']:
                    status_changes += 1
                pa = archives[i]['listing_price']
                pb = archives[i-1]['listing_price']
                if pa is not None and pb is not None and pa != pb:
                    price_changes += 1

        activity_count = status_changes + price_changes
        if activity_count > 0:
            row = {}
            for k, v in prop.items():
                if isinstance(v, bytes):
                    row[k] = v.decode('utf-8', errors='ignore')
                else:
                    row[k] = v
            row['status_changes'] = status_changes
            row['price_changes'] = price_changes
            row['activity_count'] = activity_count
            row['archive_count'] = len(archives)
            results.append(row)

    results.sort(key=lambda x: (-x['activity_count'], x['price_per_sqft']))

    cursor.close()
    conn.close()
    return jsonify(results[:10])


@app.route('/api/insights/stagnant')
def get_insight_stagnant():
    """获取挂牌时间最长的滞销房源 Top 10"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    community_id = request.args.get('community_id', '').strip()
    builder_id = request.args.get('builder_id', '').strip()

    query = """
        SELECT p.id, CAST(p.title AS CHAR) as title, p.property_uuid,
               p.community_id, p.bedrooms, p.bathrooms, p.size,
               p.price, p.status,
               p.lowest_price, p.highest_price,
               DATE_FORMAT(p.created_time, '%%Y-%%m-%%d %%H:%%i') as created_time,
               DATEDIFF(NOW(), p.created_time) as days_listed,
               cc.name as community_name, cc.uuid as community_uuid,
               b.builder_name
        FROM property p
        JOIN community_cache cc ON p.community_id = cc.id
        LEFT JOIN builder b ON cc.builder = b.builder_id
        WHERE p.status != 'Sold'
          AND p.price > 0
          AND p.title IS NOT NULL 
          AND p.title != ''
    """
    params = []
    if community_id:
        query += " AND p.community_id = %s"
        params.append(int(community_id))
    if builder_id:
        query += " AND cc.builder = %s"
        params.append(int(builder_id))
        
    query += " ORDER BY p.created_time ASC LIMIT 10"
    
    cursor.execute(query, params)
    results = cursor.fetchall()

    serializable = []
    for row in results:
        s = {}
        for k, v in row.items():
            if isinstance(v, bytes):
                s[k] = v.decode('utf-8', errors='ignore')
            else:
                s[k] = v
        serializable.append(s)

    cursor.close()
    conn.close()
    return jsonify(serializable)

# ==================== 数据智能洞察结束 ====================


if __name__ == '__main__':
    # 应用启动时尝试恢复批量采集任务（已禁用，避免重复启动采集）
    # recover_batch_collection_on_startup()
    
    debug = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', 5000))
    app.run(debug=debug, host=host, port=port)
