const fs = require('fs').promises;
const csv = require('csv-parser');
const createReadStream = require('fs').createReadStream;

/**
 * 検算用スクリプト：上岡洋一郎さんのデータで手動計算検証
 */

function parseStayTime(timeStr) {
    const [hours, minutes, seconds] = timeStr.split(':').map(Number);
    return hours * 60 + minutes + seconds / 60;
}

function parseDateTime(dateStr) {
    return new Date(dateStr);
}

function getYearMonth(date) {
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}

function isWithinPeriod(date, months) {
    const cutoffDate = new Date();
    cutoffDate.setMonth(cutoffDate.getMonth() - months);
    return date >= cutoffDate;
}

function isJulyOnly(date) {
    const yearMonth = getYearMonth(date);
    return yearMonth === '2025-07';
}

async function verifyCalculation() {
    const targetUser = '森聖子';
    const userRecords = [];
    
    console.log(`🔍 ${targetUser}さんのデータを検証します...\n`);

    // CSVデータを読み込み、対象ユーザーのみ抽出
    return new Promise((resolve, reject) => {
        createReadStream('nihouse.csv')
            .pipe(csv())
            .on('data', (row) => {
                const customerName = row['顧客名']?.trim();
                const checkinTime = row['チェックイン日時']?.trim();
                const stayTime = row['滞在時間']?.trim();

                if (customerName === targetUser && checkinTime && stayTime) {
                    try {
                        const checkinDate = parseDateTime(checkinTime);
                        const stayMinutes = parseStayTime(stayTime);

                        if (stayMinutes > 0) {
                            userRecords.push({
                                customerName,
                                checkinDate,
                                stayMinutes,
                                stayTime: stayTime,
                                yearMonth: getYearMonth(checkinDate)
                            });
                        }
                    } catch (error) {
                        console.warn('データ解析エラー:', row);
                    }
                }
            })
            .on('end', () => {
                console.log(`📊 ${targetUser}さんの有効レコード数: ${userRecords.length}件\n`);
                
                // 6ヶ月間の検証
                console.log('=== 6ヶ月間の計算検証 ===');
                verifyPeriod(userRecords, 6);
                
                console.log('\n=== 1ヶ月間の計算検証 ===');
                verifyPeriod(userRecords, 1);
                
                console.log('\n=== 時間変換の例 ===');
                showTimeConversionExamples(userRecords);
                
                resolve();
            })
            .on('error', reject);
    });
}

function verifyPeriod(allRecords, months) {
    // 期間フィルタリング
    const filteredRecords = allRecords.filter(record => {
        if (months === 1) {
            // 1ヶ月間の場合は7月のデータのみ
            return isJulyOnly(record.checkinDate);
        } else {
            // 6ヶ月間の場合は従来通り
            return isWithinPeriod(record.checkinDate, months);
        }
    });
    
    const periodName = months === 1 ? '7月' : `${months}ヶ月間`;
    console.log(`📅 ${periodName}の対象レコード: ${filteredRecords.length}件`);
    
    if (filteredRecords.length === 0) {
        console.log('該当期間にデータがありません');
        return;
    }
    
    // 月別集計
    const monthlyStats = {};
    
    filteredRecords.forEach(record => {
        const { yearMonth, stayMinutes } = record;
        
        if (!monthlyStats[yearMonth]) {
            monthlyStats[yearMonth] = {
                count: 0,
                totalMinutes: 0
            };
        }
        
        monthlyStats[yearMonth].count++;
        monthlyStats[yearMonth].totalMinutes += stayMinutes;
    });
    
    console.log('\n📈 月別集計:');
    let totalVisits = 0;
    let totalMinutes = 0;
    
    for (const yearMonth in monthlyStats) {
        const data = monthlyStats[yearMonth];
        const hours = Math.round(data.totalMinutes / 60 * 10) / 10;
        
        console.log(`  ${yearMonth}: ${data.count}回, ${hours}時間 (${data.totalMinutes.toFixed(1)}分)`);
        
        totalVisits += data.count;
        totalMinutes += data.totalMinutes;
    }
    
    const activeMonths = Object.keys(monthlyStats).length;
    const totalHours = Math.round(totalMinutes / 60 * 10) / 10;
    
    // 月平均計算
    const monthlyVisits = Math.round((totalVisits / months) * 10) / 10;
    const monthlyHours = Math.round((totalMinutes / (60 * months)) * 10) / 10;
    
    console.log('\n🧮 計算結果:');
    console.log(`  総利用回数: ${totalVisits}回`);
    console.log(`  総利用時間: ${totalHours}時間 (${totalMinutes.toFixed(1)}分)`);
    console.log(`  活動月数: ${activeMonths}ヶ月`);
    console.log(`  対象期間: ${months}ヶ月`);
    console.log(`  月平均利用回数: ${totalVisits} ÷ ${months} = ${monthlyVisits}回/月`);
    console.log(`  月平均利用時間: ${totalMinutes.toFixed(1)} ÷ (60 × ${months}) = ${monthlyHours}時間/月`);
    
    // JSONファイルの結果と比較
    compareWithJSON(months, {
        monthlyVisits,
        monthlyHours,
        totalVisits,
        totalHours,
        activeMonths
    });
}

function showTimeConversionExamples(records) {
    console.log('時間変換の例:');
    
    // 最初の5件の時間変換を表示
    records.slice(0, 5).forEach((record, index) => {
        const hours = Math.floor(record.stayMinutes / 60);
        const minutes = Math.floor(record.stayMinutes % 60);
        const seconds = Math.round((record.stayMinutes % 1) * 60);
        
        console.log(`  ${index + 1}. "${record.stayTime}" → ${record.stayMinutes.toFixed(2)}分 (${hours}時間${minutes}分${seconds}秒)`);
    });
}

async function compareWithJSON(months, calculated) {
    try {
        const jsonData = JSON.parse(await fs.readFile('../docs/user-data.json', 'utf8'));
        const dataKey = months === 6 ? 'sixMonthData' : 'oneMonthData';
        const userData = jsonData[dataKey];
        
        const targetUser = userData.find(user => user.name === '上岡洋一郎');
        
        if (targetUser) {
            console.log('\n📋 JSONファイルとの比較:');
            console.log(`  月平均利用回数: 計算値=${calculated.monthlyVisits} | JSON=${targetUser.monthlyVisits} | 一致=${calculated.monthlyVisits === targetUser.monthlyVisits ? '✅' : '❌'}`);
            console.log(`  月平均利用時間: 計算値=${calculated.monthlyHours} | JSON=${targetUser.monthlyHours} | 一致=${calculated.monthlyHours === targetUser.monthlyHours ? '✅' : '❌'}`);
            console.log(`  総利用回数: 計算値=${calculated.totalVisits} | JSON=${targetUser.totalVisits} | 一致=${calculated.totalVisits === targetUser.totalVisits ? '✅' : '❌'}`);
            console.log(`  総利用時間: 計算値=${calculated.totalHours} | JSON=${targetUser.totalHours} | 一致=${calculated.totalHours === targetUser.totalHours ? '✅' : '❌'}`);
            console.log(`  活動月数: 計算値=${calculated.activeMonths} | JSON=${targetUser.activeMonths} | 一致=${calculated.activeMonths === targetUser.activeMonths ? '✅' : '❌'}`);
        } else {
            console.log('❌ JSONファイルで対象ユーザーが見つかりませんでした');
        }
    } catch (error) {
        console.log('❌ JSONファイル読み込みエラー:', error.message);
    }
}

// スクリプト実行
if (require.main === module) {
    verifyCalculation().catch(console.error);
}