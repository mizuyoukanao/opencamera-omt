package net.sourceforge.opencamera;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.os.Build;
import android.os.IBinder;
import android.util.Log;

/**
 * Foreground service that keeps OMT streaming running while the device
 * sleeps (screen off) or the activity is otherwise in the background.
 *
 * The service does not own the camera or the OMT sender - those live in
 * MainActivity / OMTStreamingManager. Running a foreground service of type
 * "camera" is what allows the app to keep using the camera once the
 * activity is no longer visible (Android 9+ blocks camera access for
 * background apps otherwise), and raises the process priority so the
 * encoder/network threads keep running while the screen is off. CPU and
 * Wi-Fi are kept awake by the wake locks held in OMTStreamingManager.
 *
 * Start this while the app is in the foreground (required for
 * foregroundServiceType="camera" on Android 14+).
 */
public class OMTStreamingService extends Service {
    private static final String TAG = "OMTStreamingService";
    private static final String CHANNEL_ID = "omt_streaming";
    private static final int NOTIFICATION_ID = 2001;

    /** Start the foreground service (call while app is in foreground). */
    public static void start(Context context) {
        Intent intent = new Intent(context, OMTStreamingService.class);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            context.startForegroundService(intent);
        } else {
            context.startService(intent);
        }
    }

    /** Stop the foreground service and remove its notification. */
    public static void stop(Context context) {
        context.stopService(new Intent(context, OMTStreamingService.class));
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        Log.i(TAG, "onStartCommand");
        createNotificationChannel();

        Intent activityIntent = new Intent(this, MainActivity.class);
        int piFlags = Build.VERSION.SDK_INT >= Build.VERSION_CODES.M ? PendingIntent.FLAG_IMMUTABLE : 0;
        PendingIntent contentIntent = PendingIntent.getActivity(this, 0, activityIntent, piFlags);

        Notification.Builder builder;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            builder = new Notification.Builder(this, CHANNEL_ID);
        } else {
            builder = new Notification.Builder(this);
        }
        Notification notification = builder
                .setSmallIcon(R.drawable.ic_videocam_white_48dp)
                .setContentTitle(getString(R.string.omt_notification_title))
                .setContentText(getString(R.string.omt_notification_text))
                .setOngoing(true)
                .setContentIntent(contentIntent)
                .build();

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            // FOREGROUND_SERVICE_TYPE_CAMERA exists from API 30; on older
            // versions any foreground service permits background camera use.
            startForeground(NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_CAMERA);
        } else {
            startForeground(NOTIFICATION_ID, notification);
        }

        // Not sticky: streaming state lives in MainActivity, so a restarted
        // service without the activity would have nothing to do.
        return START_NOT_STICKY;
    }

    @Override
    public void onDestroy() {
        Log.i(TAG, "onDestroy");
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            stopForeground(STOP_FOREGROUND_REMOVE);
        } else {
            stopForeground(true);
        }
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(
                    CHANNEL_ID,
                    getString(R.string.omt_notification_channel),
                    NotificationManager.IMPORTANCE_LOW);
            NotificationManager notificationManager = getSystemService(NotificationManager.class);
            if (notificationManager != null) {
                notificationManager.createNotificationChannel(channel);
            }
        }
    }
}
