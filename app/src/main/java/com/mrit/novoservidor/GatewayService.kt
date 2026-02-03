package com.mrit.novoservidor

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import com.chaquo.python.Python
import com.chaquo.python.android.AndroidPlatform
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

class GatewayService : Service() {
    private val isRunning = AtomicBoolean(false)
    private val executor = Executors.newSingleThreadExecutor()

    override fun onCreate() {
        super.onCreate()
        if (!Python.isStarted()) {
            Python.start(AndroidPlatform(this))
        }
        startForeground(NOTIFICATION_ID, buildNotification())
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (isRunning.compareAndSet(false, true)) {
            executor.execute {
                try {
                    val py = Python.getInstance()
                    py.getModule("tuya_server").callAttr("main")
                } catch (ex: Exception) {
                    isRunning.set(false)
                }
            }
        }
        return START_STICKY
    }

    override fun onDestroy() {
        executor.shutdownNow()
        isRunning.set(false)
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun buildNotification(): Notification {
        val manager = getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "Tuya Gateway",
                NotificationManager.IMPORTANCE_LOW
            )
            manager.createNotificationChannel(channel)
        }
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("Tuya Gateway ativo")
            .setContentText("Servidor local em execução")
            .setSmallIcon(R.mipmap.ic_launcher)
            .setOngoing(true)
            .build()
    }

    companion object {
        private const val CHANNEL_ID = "tuya_gateway_channel"
        private const val NOTIFICATION_ID = 1001
    }
}
