#!/usr/bin/env python3
"""
Scheduled Command Executor - Execute commands at specific times
Usage: python scheduler.py
"""

import subprocess
import time
import datetime
import threading
import json
import os
from typing import List, Dict, Any

class CommandScheduler:
    def __init__(self, config_file: str = "scheduled_commands.json"):
        self.config_file = config_file
        self.scheduled_tasks = []
        self.running = False
        self.load_config()
    
    def load_config(self):
        """Load scheduled commands from JSON config file"""
        print(f"Looking for config file: {self.config_file}")
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    self.scheduled_tasks = config.get('tasks', [])
                    print(f"✓ Loaded {len(self.scheduled_tasks)} scheduled tasks from existing config")
            except Exception as e:
                print(f"✗ Error loading config: {e}")
                print("Creating new config file...")
                self.create_sample_config()
        else:
            print("Config file not found. Creating sample config...")
            self.create_sample_config()
    
    def create_sample_config(self):
        """Create a sample configuration file"""
        sample_config = {
            "tasks": [
                {
                    "name": "Daily backup",
                    "command": "echo 'Running daily backup'",
                    "time": "02:00",
                    "days": ["monday", "tuesday", "wednesday", "thursday", "friday"],
                    "enabled": False
                },
                {
                    "name": "System update check",
                    "command": "echo 'Checking for updates'",
                    "time": "09:00",
                    "days": ["sunday"],
                    "enabled": False
                },
                {
                    "name": "Log cleanup",
                    "command": "echo 'Cleaning old logs'",
                    "time": "23:30",
                    "days": ["daily"],
                    "enabled": False
                }
            ]
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(sample_config, f, indent=2)
        print(f"✓ Created sample config file: {self.config_file}")
        print("  Edit the file to add your commands and set enabled: true")
        self.scheduled_tasks = sample_config['tasks']
    
    def parse_time(self, time_str: str) -> tuple:
        """Parse time string (HH:MM) to hour and minute integers"""
        try:
            hour, minute = map(int, time_str.split(':'))
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return hour, minute
            else:
                raise ValueError("Invalid time range")
        except Exception:
            raise ValueError(f"Invalid time format: {time_str}. Use HH:MM format.")
    
    def should_run_today(self, days: List[str]) -> bool:
        """Check if task should run today based on day configuration"""
        if "daily" in days:
            return True
        
        today = datetime.datetime.now().strftime('%A').lower()
        return today in [day.lower() for day in days]
    
    def execute_command(self, task: Dict[str, Any]):
        """Execute a command safely"""
        try:
            print(f"[{datetime.datetime.now()}] Executing: {task['name']}")
            print(f"Command: {task['command']}")
            
            result = subprocess.run(
                task['command'],
                shell=True,
                capture_output=True,
                text=True,
                timeout=1200  # 5 minute timeout
            )
            
            if result.returncode == 0:
                print(f"✓ Command completed successfully")
                if result.stdout:
                    print(f"Output: {result.stdout.strip()}")
            else:
                print(f"✗ Command failed with return code {result.returncode}")
                if result.stderr:
                    print(f"Error: {result.stderr.strip()}")
                    
        except subprocess.TimeoutExpired:
            print(f"✗ Command timed out after 20 minutes")
        except Exception as e:
            print(f"✗ Error executing command: {e}")
        
        print("-" * 50)
    
    def check_and_execute(self):
        """Check if any commands should be executed now"""
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M")
        
        for task in self.scheduled_tasks:
            if not task.get('enabled', False):
                continue
            
            if task['time'] == current_time and self.should_run_today(task['days']):
                # Execute in a separate thread to avoid blocking
                thread = threading.Thread(
                    target=self.execute_command, 
                    args=(task,)
                )
                thread.start()
    
    def run(self):
        """Main scheduler loop"""
        self.running = True
        print("Command scheduler started...")
        print("Press Ctrl+C to stop")
        
        try:
            while self.running:
                self.check_and_execute()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            print("\nScheduler stopped by user")
        except Exception as e:
            print(f"Scheduler error: {e}")
        finally:
            self.running = False
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
    
    def list_tasks(self):
        """List all configured tasks"""
        print("\nConfigured tasks:")
        print("=" * 60)
        
        for i, task in enumerate(self.scheduled_tasks, 1):
            status = "✓ Enabled" if task.get('enabled', False) else "✗ Disabled"
            days_str = ", ".join(task['days'])
            
            print(f"{i}. {task['name']} ({status})")
            print(f"   Time: {task['time']}")
            print(f"   Days: {days_str}")
            print(f"   Command: {task['command']}")
            print()
    
    def add_task(self, name: str, command: str, time_str: str, days: List[str]):
        """Add a new scheduled task"""
        # Validate time format
        self.parse_time(time_str)
        
        new_task = {
            "name": name,
            "command": command,
            "time": time_str,
            "days": days,
            "enabled": True
        }
        
        self.scheduled_tasks.append(new_task)
        self.save_config()
        print(f"Task '{name}' added successfully")
    
    def save_config(self):
        """Save current configuration to file"""
        config = {"tasks": self.scheduled_tasks}
        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=2)

def main():
    try:
        print("Initializing Command Scheduler...")
        scheduler = CommandScheduler()
        print("Scheduler initialized successfully!")
        
        # Simple CLI interface
        while True:
            try:
                print("\n" + "="*50)
                print("Scheduled Command Executor")
                print("="*50)
                print("1. Start scheduler")
                print("2. List tasks")
                print("3. Add task")
                print("4. Edit config file")
                print("5. Exit")
                print("-"*50)
                
                choice = input("Enter your choice (1-5): ").strip()
                
                if choice == '1':
                    print("\nStarting scheduler...")
                    scheduler.run()
                elif choice == '2':
                    scheduler.list_tasks()
                elif choice == '3':
                    try:
                        print("\nAdding new task:")
                        name = input("Task name: ")
                        command = input("Command to execute: ")
                        time_str = input("Time (HH:MM format): ")
                        days_input = input("Days (comma-separated, or 'daily'): ")
                        days = [day.strip() for day in days_input.split(',')]
                        
                        scheduler.add_task(name, command, time_str, days)
                    except KeyboardInterrupt:
                        print("\nCancelled by user")
                    except Exception as e:
                        print(f"Error adding task: {e}")
                elif choice == '4':
                    print(f"\nConfig file location: {scheduler.config_file}")
                    print("Edit this file with your preferred text editor")
                    print("Then restart the program to load changes")
                    input("Press Enter to continue...")
                elif choice == '5':
                    print("Goodbye!")
                    break
                else:
                    print("Invalid choice. Please enter 1-5.")
                    
            except KeyboardInterrupt:
                print("\n\nExiting...")
                break
            except EOFError:
                print("\n\nInput stream closed. Exiting...")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                print("Continuing...")
                
    except Exception as e:
        print(f"Fatal error during initialization: {e}")
        input("Press Enter to exit...")

if __name__ == "__main__":
    print("Starting Scheduled Command Executor...")
    try:
        main()
    except Exception as e:
        print(f"Program crashed: {e}")
        input("Press Enter to exit...")
    finally:
        print("Program ended.")